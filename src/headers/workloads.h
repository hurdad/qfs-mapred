/*
 * workloads.h
 *
 *  Created on: Nov 17, 2012
 *      Author: alex
 */

#ifndef WORKLOADS_H_
#define WORKLOADS_H_

#include "json_spirit_headers/json_spirit_value.h"

struct Mapper_Workload
{
	std::string job_id;
	std::string qfs_meta_server_name;
	unsigned short qfs_meta_server_port;
	std::string qfs_file_input;
	std::string qfs_map_folder;
    std::string python_mapper_function;
    int partition_count;
};

struct Sorter_Workload
{
	std::string qfs_meta_server_name;
	unsigned short qfs_meta_server_port;
	std::string qfs_partition_file_input;
	std::string qfs_partition_file_output;
};

struct Reducer_Workload
{
	std::string qfs_meta_server_name;
	unsigned short qfs_meta_server_port;
	json_spirit::Array qfs_sorted_partition_files;
	std::string qfs_output_file;
    std::string python_reducer_function;
};

#endif /* WORKLOADS_H_ */
