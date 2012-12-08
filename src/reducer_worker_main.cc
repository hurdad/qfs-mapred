/*
 ============================================================================
 Name        : reducer_worker.cpp
 Author      : Alexander Hurd
 Version     :
 Copyright   :
 Description :
 ============================================================================
 */

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

static void *reducer(gearman_job_st *job, void *context, size_t *result_size,
		gearman_return_t *ret_ptr);

string path_to_qfs_bin_tools;
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
			"Path to qfs tools folder");
	;

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

	ret = gearman_worker_add_function(&worker, "reducer", 0, reducer, NULL);
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

static void *reducer(gearman_job_st *job, void *context, size_t *result_size,
		gearman_return_t *ret_ptr) {

	const char *workload;
	workload = (const char *) gearman_job_workload(job);

	//build payload
	string json_workload = (string) workload;
	Value value;
	read_string(json_workload, value);
	Object obj = value.get_obj();
	Reducer_Workload payload;
	for (Object::size_type i = 0; i != obj.size(); ++i) {
		const Pair& pair = obj[i];

		const string& name = pair.name_;
		const Value& value = pair.value_;

		if (name == "qfs_meta_server_name") {
			payload.qfs_meta_server_name = value.get_str();
		} else if (name == "qfs_meta_server_port") {
			payload.qfs_meta_server_port = value.get_int();
		} else if (name == "qfs_sorted_partition_file") {
			payload.qfs_sorted_partition_files = value.get_array();
		} else if (name == "qfs_output_file") {
			payload.qfs_output_file = value.get_str();
		} else if (name == "python_reducer_function") {
			payload.python_reducer_function = value.get_str();
		} else {
			assert( false);
		}
	}

	//write reducer function to file disk
	const char *python_reducer_file = tmpnam(NULL);  // Get temp name
	FILE *fp = fopen(python_reducer_file, "w");  // Create the file
	fwrite(payload.python_reducer_function.c_str(), 1,
			payload.python_reducer_function.size(), fp);
	fclose(fp);

	printf("Checking if processor is available...");
	if (!system(NULL)) {
		// *ret_ptr = GEARMAN_ERROR;
		// return;
	}

	//loop sorted partitions and reduce!
	for (int i = 0; i < payload.qfs_sorted_partition_files.size(); i++) {

		string file = payload.qfs_sorted_partition_files[i].get_str();

		int r;
		string mapper_command = path_to_qfs_bin_tools + "cpfromqfs -s "
				+ payload.qfs_meta_server_name + " -p "
				+ boost::lexical_cast<string>(payload.qfs_meta_server_port)
				+ " -k " + file
				+ " -d - | python " + python_reducer_file + " | " + path_to_qfs_bin_tools + "cptoqfs -s "
				+ payload.qfs_meta_server_name + " -p "
				+ boost::lexical_cast<string>(payload.qfs_meta_server_port)
				+ " -d - -k " + payload.qfs_output_file + "_" + boost::lexical_cast<string>(i);

		r = system(mapper_command.c_str());

		//output results if any
		cout << r;
	}

	//remove python script
	unlink(python_reducer_file);

	*ret_ptr = GEARMAN_SUCCESS;

	return NULL;
}
