/*
 * shared_mapper_data.cc
 *
 *  Created on: Nov 22, 2012
 *      Author: alex
 */

#ifndef SHARED_MAPPER_DATA_H_
#define SHARED_MAPPER_DATA_H_

#include <map>
#include <list>
#include <boost/thread.hpp>

#include "shared_queue.hpp"
#include "qfs_atomic_writer.hpp"
#include "headers/mapper_data_struct.h"
#include "headers/kv_struct.h"

#define QFS_WRITER_THREADS 1

using namespace std;

class shared_mapper_data {

private:
	boost::thread_group m_writers;
	map<int, list<kv> > m_mapper_data;
	map<int, long> m_partition_sizes;
	int m_partition_count;
	long m_spill_threshold_kb;
	shared_queue<mapper_data> m_write_queue;
	boost::mutex m_mutex;

public:
	shared_mapper_data(string qfs_meta_server_host, in_port_t qfs_meta_server_port, string qfs_map_folder, string job_id, int partition_count,
			long spill_threshold_kb) :
			m_partition_count(partition_count), m_spill_threshold_kb(
					spill_threshold_kb) {

		// Create qfs writers
		for (int i = 0; i < QFS_WRITER_THREADS; i++) {
			qfs_atomic_writer w(&m_write_queue, qfs_meta_server_host, qfs_meta_server_port, qfs_map_folder, job_id);
			m_writers.create_thread(w);
		}

	}

	~shared_mapper_data() {
		m_writers.interrupt_all();
		m_writers.join_all();
	}

	//scan mapper partition data for spill threshold
	void scan() {

		//get lock
		boost::unique_lock<boost::mutex> lock(m_mutex);

		//scan partition size
		map<int, long>::iterator it;
		for (it = m_partition_sizes.begin(); it != m_partition_sizes.end();
				it++) {
			int partition = (*it).first;
			long size_bytes = (*it).second;

			//spill to qfs if threshold is met
			if (size_bytes >= m_spill_threshold_kb){
				write_partition_to_qfs(partition);
				m_partition_sizes[partition] = 0;
			}
		}

		//release
		//lock.release();
	}

	//spill to qfs
	void write_partition_to_qfs(int partition) {

		//get data from map
		list<kv> my_list = m_mapper_data[partition];
		string data;
		list<kv>::iterator it;
		for (it = my_list.begin(); it != my_list.end(); it++) {
			string key = (*it).key;
			string value = (*it).value;
			data.append(key + "\t" + value + "\n");
		}

		//empty list
		m_mapper_data[partition].clear();

		mapper_data my_data;
		my_data.data = data;
		my_data.partition = partition;

		//add to write queue
		m_write_queue.Enqueue(my_data);

	}

	//add new data
	void add(int partition, kv my_kv) {

		//get lock
		boost::unique_lock<boost::mutex> lock(m_mutex);

		//add kv to partition
		m_mapper_data[partition].push_back(my_kv);

		//increment partition size
		if (m_partition_sizes.find(partition) != m_partition_sizes.end()) {
			m_partition_sizes[partition] += my_kv.key.size()
					+ my_kv.value.size();
		} else {
			//init
			m_partition_sizes[partition] = my_kv.key.size()
					+ my_kv.value.size();
		}
	}

	//flush all data to qfs
	void flush_all() {

		//get lock
		boost::unique_lock<boost::mutex> lock(m_mutex);

		//loop partitions
		map<int, list<kv> >::iterator it;
		for (it = m_mapper_data.begin(); it != m_mapper_data.end(); it++) {
			int partition = (*it).first;
			list<kv> map_list = (*it).second;

			//atomic append list to partition file
			write_partition_to_qfs(partition);
		}
	}
};

#endif /*SHARED_MAPPER_DATA_H_*/
