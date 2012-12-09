/*
 * qfs_atomic_writer.hpp
 *
 *  Created on: Nov 23, 2012
 *      Author: alex
 */
#include <map>
#include <fstream>
#include <fcntl.h>
#include "kfs/KfsClient.h"
#include "kfs/KfsAttr.h"
#include "kfs/common/kfstypes.h"
#include "shared_queue.hpp"
#include "headers/mapper_data_struct.h"

using namespace std;

// Class that consumes objects from a queue
class qfs_atomic_writer {
private:
	shared_queue<mapper_data>* m_queue; // The queue to use
	string m_qfs_map_folder;
	string m_job_id;
	map<int, int> m_qfs_partition_fd;
	KFS::KfsClient *m_KfsClient;

public:
	qfs_atomic_writer(shared_queue<mapper_data>* queue,
			string qfs_meta_server_host, in_port_t qfs_meta_server_port,
			string qfs_map_folder, string job_id) :
			m_queue(queue), m_qfs_map_folder(qfs_map_folder), m_job_id(job_id) {

		//connect to qfs
		m_KfsClient = KFS::Connect(qfs_meta_server_host, qfs_meta_server_port);
		if (!m_KfsClient) {
			cerr << "kfs client failed to initialize...exiting" << "\n";
			exit(-1);
		}
	}

	~qfs_atomic_writer() {

		//close partitions fds
		map<int, int>::iterator it;
		for (it = m_qfs_partition_fd.begin(); it != m_qfs_partition_fd.end();
				it++) {
			int fd = (*it).second;
			m_KfsClient->Close(fd);
		}
	}

	// The thread function reads data from the queue
	void operator ()() {
		while (true) {

			// Get the data from the queue
			mapper_data data = m_queue->Dequeue();

			string my_data = data.data;
			int partition = data.partition;

			//check for fd
			int fd;
			if (m_qfs_partition_fd.find(partition)
					== m_qfs_partition_fd.end()) {
				//init fd
				init_qfs_fd(partition);
			}

			//get partition fd
			fd = m_qfs_partition_fd[partition];

			//write!
			int res = m_KfsClient->AtomicRecordAppend(fd, my_data.c_str(), my_data.size());

			//check failure
			if (res < 0)
				cout << "AtomicRecordAppend failed, with error: "
					 << res << endl;

			//check size
			if (res != (int) my_data.size())
				cout << "Atomic write err" << endl;

			// Make sure we can be interrupted
			boost::this_thread::interruption_point();
			boost::this_thread::yield();

		}
	}

	void init_qfs_fd(int partition) {
		string mapper_partition_file = m_qfs_map_folder
				+ m_job_id + "_"
				+ boost::lexical_cast<string>(partition);

		int fd;
		if ((fd = m_KfsClient->Open(mapper_partition_file.c_str(), O_CREAT|O_APPEND|O_WRONLY))
				< 0) {
			cout << "Mapper Partition File Open on : " << mapper_partition_file
					<< " failed: " << KFS::ErrorCodeToStr(fd) << endl;
			exit(-1);
		}

		//save
		m_qfs_partition_fd[partition] = fd;

	}
};

