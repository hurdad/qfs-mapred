/*
 ============================================================================
 Name        : qfs_mapred_submit.cpp
 Author      : Alexander Hurd
 Version     :
 Copyright   : Your copyright notice
 Description :
 ============================================================================
 */

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <cerrno>
#include <stdio.h>
#include <map>
#include <list>
#include <libgearman/gearman.h>
#include <boost/program_options.hpp>


#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "json_spirit_headers/json_spirit_writer_template.h"
#include "kfs/KfsClient.h"
#include "kfs/KfsAttr.h"
#include "kfs/common/kfstypes.h"

#ifndef __INTEL_COMPILER
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

static void read_file(std::string file_path, std::string &ret);
static gearman_return_t created(gearman_task_st *task);
static gearman_return_t data(gearman_task_st *task);
static gearman_return_t status(gearman_task_st *task);
static gearman_return_t complete(gearman_task_st *task);
static gearman_return_t fail(gearman_task_st *task);

using namespace std;
using namespace json_spirit;
KFS::KfsClient *gKfsClient;

int main(int args, char *argv[]) {

	//init vars
	in_port_t gearmand_port, qfs_meta_server_port;
	string gearmand_host, qfs_meta_server_host;
	int timeout, num_reducers, num_partitions;
	string qfs_input_folder, qfs_map_folder, qfs_output_folder, mapper, reducer;

	//parse cli options
	boost::program_options::options_description desc("Options");
	desc.add_options()("help", "Options related to the program.")(
			"gearmand_host",
			boost::program_options::value<string>(&gearmand_host)->default_value(
					"localhost"), "Connect to the gearmand host")(
			"gearmand_port",
			boost::program_options::value<in_port_t>(&gearmand_port)->default_value(
					GEARMAN_DEFAULT_TCP_PORT),
			"Port number use for gearmand connection")("timeout",
			boost::program_options::value<int>(&timeout)->default_value(-1),
			"Gearmand Timeout in milliseconds")("meta_server_host",
			boost::program_options::value<string>(&qfs_meta_server_host)->default_value(
					"localhost"), "Connect to the qfs meta server host")(
			"meta_server_port",
			boost::program_options::value<in_port_t>(&qfs_meta_server_port)->default_value(
					20000), "Port number use for qfs meta server connection")(
			"qfs_input_folder",
			boost::program_options::value<string>(&qfs_input_folder)->required(), "Input folder in qfs")("qfs_map_folder",
			boost::program_options::value<string>(&qfs_map_folder)->default_value(
					"/tmp/"), "Intermediate folder for storing mapper output")(
			"qfs_output_folder",
			boost::program_options::value<string>(&qfs_output_folder)->default_value(
					"/output/"), "Output folder in qfs")("mapper",
			boost::program_options::value<string>(&mapper),
			"Python Mapper script")("reducer",
			boost::program_options::value<string>(&reducer),
			"Python Reducer script")("num_reducers",
			boost::program_options::value<int>(&num_reducers)->default_value(2),
			"Number of reducers")("num_partitions",
			boost::program_options::value<int>(&num_partitions)->default_value(
					4), "Number of mapped output partitions");

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

	//read in mapper script
	string mapper_function;
	read_file(mapper, mapper_function);

	//read in reducer script
	string reducer_function;
	read_file(reducer, reducer_function);

	// Connect to qfs
	gKfsClient = KFS::Connect(qfs_meta_server_host, qfs_meta_server_port);
	if (!gKfsClient) {
		cerr << "qfs client failed to initialize...exiting" << "\n";
		return EXIT_FAILURE;
	}

	// Get the directory listings
	vector<string> entries;
	int res;
	if ((res = gKfsClient->Readdir(qfs_input_folder.c_str(), entries)) < 0) {
		cout << "Readdir failed! " << KFS::ErrorCodeToStr(res) << endl;
		return EXIT_FAILURE;
	}

	// Read dir (2 for '.' and '..')
	if (entries.size() <= 2) {
		cout << "No Input files " << KFS::ErrorCodeToStr(res) << endl;
		return EXIT_FAILURE;
	}

	//init gearman client
	gearman_client_st client;
	if (gearman_client_create(&client) == NULL) {
		cerr << "Memory allocation failure on client creation" << endl;
		return EXIT_FAILURE;
	}

	if (timeout >= 0)
		gearman_client_set_timeout(&client, timeout);

	gearman_return_t ret;
	ret = gearman_client_add_server(&client, gearmand_host.c_str(),
			gearmand_port);
	if (ret != GEARMAN_SUCCESS) {
		cerr << gearman_client_error(&client) << endl;
		return EXIT_FAILURE;
	}

	//create random job id
	boost::uuids::uuid uuid = boost::uuids::random_generator()();
	string job_id = boost::lexical_cast<std::string>(uuid);

	cout << "job_id: " << job_id << endl;

	//init mapper output partitions in qfs
	for (int i = 0; i < num_partitions; i++) {

		//create mapper partition with RS encoding
		string mapper_partition_file = qfs_map_folder
				+ job_id + "_"
				+ boost::lexical_cast<string>(i);

		int fd;
		if (( fd = gKfsClient->Create(mapper_partition_file.c_str())) < 0) {
			cout << "Partition file Create failed: " << KFS::ErrorCodeToStr(fd)
					<< endl;
			exit(-1);
		}

		//close handle
		gKfsClient->Close(fd);
	}

	//scan folder for input files
	//build mapper tasks
	string mapper_workloads[entries.size()];
	for (size_t i = 0; i < entries.size(); i++) {
		if (entries[i] == "." || entries[i] == "..")
			continue;

		//build payload
		Object payload;
		payload.push_back(Pair("job_id", job_id));
		payload.push_back(Pair("qfs_meta_server_name", qfs_meta_server_host));
		payload.push_back(Pair("qfs_meta_server_port", qfs_meta_server_port));
		payload.push_back(
				Pair("qfs_file_input", qfs_input_folder + entries[i]));
		payload.push_back(Pair("qfs_map_folder", qfs_map_folder));
		payload.push_back(Pair("python_mapper_function", mapper_function));
		payload.push_back(Pair("partition_count", num_partitions));

		//convert payload to json
		mapper_workloads[i] = write_string(Value(payload));

		//add task for each file
		if (gearman_client_add_task(&client, NULL, NULL, "mapper", NULL,
				mapper_workloads[i].c_str(), mapper_workloads[i].size(), &ret) == NULL
				|| ret != GEARMAN_SUCCESS) {
			cerr << gearman_client_error(&client) << endl;
			gearman_client_free(&client);
			return EXIT_FAILURE;
		}
	}

	//add callbacks
	gearman_client_set_created_fn(&client, created);
	gearman_client_set_data_fn(&client, data);
	gearman_client_set_status_fn(&client, status);
	gearman_client_set_complete_fn(&client, complete);
	gearman_client_set_fail_fn(&client, fail);

	//run mapper tasks (blocking until all tasks finished)
	cout << "running mapper tasks" << endl;
	ret = gearman_client_run_tasks(&client);
	if (ret != GEARMAN_SUCCESS) {
		cerr << gearman_client_error(&client) << endl;
		gearman_client_free(&client);
		return EXIT_FAILURE;
	}

	//build sorters tasks, one per partition
	string sorter_workloads[num_partitions];
	for (int i = 0; i < num_partitions; i++) {

		//build payload
		Object payload;

		payload.push_back(Pair("qfs_meta_server_name", qfs_meta_server_host));
		payload.push_back(Pair("qfs_meta_server_port", qfs_meta_server_port));
		string mapper_partition_file = qfs_map_folder
				+ boost::lexical_cast<string>(job_id) + "_"
				+ boost::lexical_cast<string>(i);
		payload.push_back(
				Pair("qfs_partition_file_input", mapper_partition_file));
		payload.push_back(
				Pair("qfs_partition_file_output",
						mapper_partition_file + "_sorted"));

		//convert payload to json
		sorter_workloads[i] = write_string(Value(payload));

		if (gearman_client_add_task(&client, NULL, NULL, "sorter", NULL,
				sorter_workloads[i].c_str(), sorter_workloads[i].size(), &ret) == NULL
				|| ret != GEARMAN_SUCCESS) {
			cerr << gearman_client_error(&client) << endl;
			gearman_client_free(&client);
			return EXIT_FAILURE;
		}
	}

	//run sort tasks (blocking until all tasks finished)
	cout << "running sorter tasks" << endl;
	ret = gearman_client_run_tasks(&client);
	if (ret != GEARMAN_SUCCESS) {
		cerr << gearman_client_error(&client) << endl;
		gearman_client_free(&client);
		return EXIT_FAILURE;
	}

	//map partitions to reducers
	map<int, Array > reducer_map;
	for (int i = 0; i < num_partitions; i++) {

		int reducer = i % num_reducers ;
		string qfs_sorted_partition_file = qfs_map_folder
				+ boost::lexical_cast<string>(job_id) + "_"
				+ boost::lexical_cast<string>(i) + "_sorted";

		reducer_map[reducer].push_back(qfs_sorted_partition_file);
	}

	//build reducers tasks
	string reducer_workloads[num_reducers];
	for(int i = 0; i < num_reducers; i++){

		//build payload
		Object payload;

		payload.push_back(Pair("qfs_meta_server_name", qfs_meta_server_host));
		payload.push_back(Pair("qfs_meta_server_port", qfs_meta_server_port));
		payload.push_back(
				Pair("qfs_sorted_partition_file", reducer_map[i]));
		payload.push_back(Pair("qfs_output_file", qfs_output_folder + boost::lexical_cast<string>(job_id) + "_part" + boost::lexical_cast<string>(i)));
		payload.push_back(Pair("python_reducer_function", reducer_function));

		//convert payload to json
		reducer_workloads[i] = write_string(Value(payload));

		if (gearman_client_add_task(&client, NULL, NULL, "reducer", NULL,
				reducer_workloads[i].c_str(), reducer_workloads[i].size(), &ret) == NULL
				|| ret != GEARMAN_SUCCESS) {
			cerr << gearman_client_error(&client) << endl;
			gearman_client_free(&client);
			return EXIT_FAILURE;
		}
	}

	//run reducer tasks (blocking until all tasks finished)
	cout << "running reducer tasks" << endl;
	ret = gearman_client_run_tasks(&client);
	if (ret != GEARMAN_SUCCESS) {
		cerr << gearman_client_error(&client) << endl;
		gearman_client_free(&client);
		return EXIT_FAILURE;
	}


	//concatentate reducer results

	//"qfscat  -s localhost -p 20000 /output/41ebea67-9ac9-45c7-839d-d83d9fe2490f_part0_0 "
	//"								/output/41ebea67-9ac9-45c7-839d-d83d9fe2490f_part0_1 "
	//"								/output/41ebea67-9ac9-45c7-839d-d83d9fe2490f_part1_0 /output/41ebea67-9ac9-45c7-839d-d83d9fe2490f_part1_1 "
	//"| cptoqfs -s localhost -p 20000 -d - -k /output/41ebea67-9ac9-45c7-839d-d83d9fe2490f_out"



	//remove intermediate data


	gearman_client_free(&client);

	return EXIT_SUCCESS;

}

static void read_file(string file_path, string &ret) {

	FILE * pFile;
	size_t lSize;
	char * buffer;
	size_t result;

	pFile = fopen(file_path.c_str(), "rb");
	if (pFile == NULL) {
		fputs("File error", stderr);
		exit(1);
	}

	// obtain file size:
	fseek(pFile, 0, SEEK_END);
	lSize = ftell(pFile);
	rewind(pFile);

	// allocate memory to contain the whole file:
	buffer = (char*) malloc(sizeof(char) * lSize);
	if (buffer == NULL) {
		fputs("Memory error", stderr);
		exit(2);
	}

	// copy the file into the buffer:
	result = fread(buffer, 1, lSize, pFile);
	if (result != lSize) {
		fputs("Reading error", stderr);
		exit(3);
	}

	/* the whole file is now loaded in the memory buffer. */
	ret = (string) buffer;

	// terminate
	fclose(pFile);
	free(buffer);
}

static gearman_return_t created(gearman_task_st *task) {
	cout << "Created: " << gearman_task_job_handle(task) << endl;

	return GEARMAN_SUCCESS;
}

static gearman_return_t data(gearman_task_st *task) {
	cout << "Data: " << gearman_task_job_handle(task) << " ";
	cout.write((char *) gearman_task_data(task), gearman_task_data_size(task));
	cout << endl;

	return GEARMAN_SUCCESS;
}

static gearman_return_t status(gearman_task_st *task) {
	clog << "Status: " << gearman_task_job_handle(task) << " ("
			<< gearman_task_numerator(task) << "/"
			<< gearman_task_denominator(task) << ")" << endl;

	return GEARMAN_SUCCESS;
}

static gearman_return_t complete(gearman_task_st *task) {
	cout << "Completed: " << gearman_task_job_handle(task) << " ";
	cout.write((char *) gearman_task_data(task), gearman_task_data_size(task));
	cout << endl;

	return GEARMAN_SUCCESS;
}

static gearman_return_t fail(gearman_task_st *task) {
	cerr << "Failed: " << gearman_task_job_handle(task) << endl;
	return GEARMAN_SUCCESS;
}
