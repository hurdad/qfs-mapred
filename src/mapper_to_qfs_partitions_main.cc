#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <stdio.h>
#include <iostream>
#include <string>
#include <map>
#include <list>
#include <boost/crc.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/thread.hpp>
//add in_port_t type
#include <libgearman/gearman.h>

#include "kfs/KfsClient.h"
#include "kfs/KfsAttr.h"

#include "headers/kv_struct.h"
#include "shared_mapper_data.cc"
#include "mapper_spill_checker.hpp"

using namespace std;
KFS::KfsClient *gKfsClient;

#define SPILL_THRESHOLD_BYTES 1048576 //1MB

int crc32(const string& my_string);
int get_partition(string key, int num_partitions);

int main(int args, char *argv[]) {
	in_port_t qfs_meta_server_port;
	int  partition_count;
	string qfs_meta_server_host, qfs_map_folder, job_id;

	boost::program_options::options_description desc("Options");
	desc.add_options()("help", "Options related to the program.")
			("meta_server_host,s", boost::program_options::value<string>(&qfs_meta_server_host)->default_value(	"localhost"), "Connect to the qfs meta server host")
			("meta_server_port,p", boost::program_options::value<in_port_t>(&qfs_meta_server_port)->default_value(20000), "Port number use for qfs meta server connection")
			("qfs_map_folder,f", boost::program_options::value<string>(&qfs_map_folder)->default_value("/tmp/"), "Intermediate folder for storing mapper output")
			("job_id", boost::program_options::value<string>(&job_id)->required(), "MR Job id")
			("partition_count,c", boost::program_options::value<int>(&partition_count)->default_value(4), "Number of mapped output partitions");

	boost::program_options::variables_map vm;
	try {
		boost::program_options::store(
				boost::program_options::parse_command_line(args, argv, desc),
				vm);
		boost::program_options::notify(vm);
	} catch (exception &e) {
		cout << e.what() << endl;
		return EXIT_FAILURE;
	}

	if (vm.count("help")) {
		cout << desc << endl;
		return EXIT_SUCCESS;
	}

	//init
	string input_line;
	shared_mapper_data mapper_data(qfs_meta_server_host, qfs_meta_server_port, qfs_map_folder, job_id, partition_count, SPILL_THRESHOLD_BYTES);

	//start mapper_data spill thread
	mapper_spill_checker c(&mapper_data);
	boost::thread t(c);

	while (getline(cin, input_line)) {

		//split on tab
		vector<string> input;
		boost::split(input, input_line, boost::is_any_of("\t"));

		//get <key> <value>
		string key = input[0];
		string value = input[1];

		//build kv struct
		kv my_kv;
		my_kv.key = key;
		my_kv.value = value;

		//partition
		int partition = get_partition(my_kv.key, partition_count);

		//save
		mapper_data.add(partition, my_kv);

	}

	//flush any remaining mapper_data
	mapper_data.flush_all();

	//wait for thread
	t.interrupt();
	t.join();

	return 0;
}

//crc calculation
int crc32(const string& my_string) {
    boost::crc_32_type result;
    result.process_bytes(my_string.data(), my_string.length());
    return result.checksum();
}

//simple random hash partitioning
int get_partition(string key, int num_partitions){
	return abs(crc32(key)) % num_partitions;
}

