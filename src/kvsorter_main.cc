/*
 * kvsorter_main.cc
 *
 *  Created on: Nov 21, 2012
 *      Author: alex
 *
 * In memory key value sorter using stdin and stdout
 */
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include "headers/kv_struct.h"

using namespace std;

bool comparator(kv t1, kv t2);

int main(int args, char *argv[]) {

	string input_line;
	vector<kv> data;
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

		//populate data vector
		data.push_back(my_kv);
	}

	//sort data vector with stl
	sort(data.begin(),data.end(), comparator);

	//print sorted data to stdout
	vector<kv>::iterator it;
	for (it = data.begin(); it != data.end(); it++) {
		string key = (*it).key;
		string value = (*it).value;

		printf("%s\t%s\n", key.c_str(), value.c_str());
	}

	return 0;
}

bool comparator(kv t1, kv t2)
{
   return t1.key < t2.key ? true : false;
}
