#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include <libgearman/gearman.h>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>

#include "json_spirit_headers/json_spirit_reader_template.h"
#include "headers/workloads.h"

#ifndef __INTEL_COMPILER
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

using namespace std;
using namespace json_spirit;

static void *mapper(gearman_job_st *job, void *context, size_t *result_size,
		gearman_return_t *ret_ptr);

string path_to_qfs_bin_tools, path_to_qfs_mapred_bin;
int main(int args, char *argv[]) {
	uint32_t count;
	int timeout;
	in_port_t port;
	string host;

	boost::program_options::options_description desc("Options");
	desc.add_options()("help", "Options related to the program.")("host,h",
			boost::program_options::value<string>(&host)->default_value(
					"localhost"), "Connect to the gearmand host")("port,p",
			boost::program_options::value<in_port_t>(&port)->default_value(
					GEARMAN_DEFAULT_TCP_PORT),
			"Port number use for gearmand connection")("count,c",
			boost::program_options::value<uint32_t>(&count)->default_value(0),
			"Number of jobs to run before exiting")("timeout,u",
			boost::program_options::value<int>(&timeout)->default_value(-1),
			"Timeout in milliseconds")("path_to_qfs_bin_tools",
			boost::program_options::value<string>(&path_to_qfs_bin_tools)->default_value(
					"/opt/qc/qfs/client/bin/"),
			"Path to qfs tools folder")("path_to_qfs_mapred_bin",
			boost::program_options::value<string>(&path_to_qfs_mapred_bin)->default_value(
					"/opt/hurdad/qfs-mapred/bin/"),
			"Path to qfs_mapred bin folder");

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

	gearman_worker_st worker;
	if (gearman_worker_create(&worker) == NULL) {
		cerr << "Memory allocation failure on worker creation." << endl;
		return EXIT_FAILURE;
	}

	if (timeout >= 0)
		gearman_worker_set_timeout(&worker, timeout);

	gearman_return_t ret;
	ret = gearman_worker_add_server(&worker, host.c_str(), port);
	if (ret != GEARMAN_SUCCESS) {
		cerr << gearman_worker_error(&worker) << endl;
		return EXIT_FAILURE;
	}

	ret = gearman_worker_add_function(&worker, "mapper", 0, mapper, NULL);
	if (ret != GEARMAN_SUCCESS) {
		cerr << gearman_worker_error(&worker) << endl;
		return EXIT_FAILURE;
	}

	while (1) {
		ret = gearman_worker_work(&worker);
		if (ret != GEARMAN_SUCCESS) {
			cerr << gearman_worker_error(&worker) << endl;
			break;
		}

		if (count > 0) {
			count--;
			if (count == 0)
				break;
		}
	}

	gearman_worker_free(&worker);

	return EXIT_SUCCESS;
}

static void *mapper(gearman_job_st *job, void *context, size_t *result_size,
		gearman_return_t *ret_ptr) {

	const char *workload;
	workload = (const char *) gearman_job_workload(job);

	//build payload
	string json_workload = (string) workload;
	Value value;
	read_string(json_workload, value);
	Object obj = value.get_obj();
	Mapper_Workload payload;
	for (Object::size_type i = 0; i != obj.size(); ++i) {
		const Pair& pair = obj[i];

		const string& name = pair.name_;
		const Value& value = pair.value_;

		if (name == "job_id") {
			payload.job_id = value.get_str();
		} else if (name == "qfs_meta_server_name") {
			payload.qfs_meta_server_name = value.get_str();
		} else if (name == "qfs_meta_server_port") {
			payload.qfs_meta_server_port = value.get_int();
		} else if (name == "qfs_file_input") {
			payload.qfs_file_input = value.get_str();
		} else if (name == "qfs_map_folder") {
			payload.qfs_map_folder = value.get_str();
		} else if (name == "python_mapper_function") {
			payload.python_mapper_function = value.get_str();
		} else if (name == "partition_count") {
			payload.partition_count = value.get_int();
		} else {
			assert( false);
		}
	}

	//write maper function to file disk
	const char *python_mapper_file = tmpnam(NULL);  // Get temp name
	FILE *fp = fopen(python_mapper_file, "w");  // Create the file
	fwrite(payload.python_mapper_function.c_str(), 1,
			payload.python_mapper_function.size(), fp);
	fclose(fp);

	printf("Checking if processor is available...");
	if (!system(NULL)) {
		// *ret_ptr = GEARMAN_ERROR;
		// return;
	}

	int r;
	string mapper_command = path_to_qfs_bin_tools + "cpfromqfs -s "
			+ payload.qfs_meta_server_name + " -p "
			+ boost::lexical_cast<string>(payload.qfs_meta_server_port) + " -k "
			+ payload.qfs_file_input + " -d - | python " + python_mapper_file
			+ " | " + path_to_qfs_mapred_bin + "mapper_to_qfs_partitions -s " + payload.qfs_meta_server_name
			+ " -p " + boost::lexical_cast<string>(payload.qfs_meta_server_port)
			+ " -f " + payload.qfs_map_folder + " --job_id " + payload.job_id
			+ " --partition_count "
			+ boost::lexical_cast<string>(payload.partition_count);
	r = system(mapper_command.c_str());

	//output results if any
	cout << r;

	//remove python script
	unlink(python_mapper_file);

	*ret_ptr = GEARMAN_SUCCESS;

	return NULL;

}

