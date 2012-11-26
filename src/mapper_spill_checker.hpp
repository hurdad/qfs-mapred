/*
 * mapper_spill_checker.hpp
 *
 *  Created on: Nov 23, 2012
 *      Author: alex
 */

#include "shared_mapper_data.cc"
class mapper_spill_checker {
private:
	shared_mapper_data *m_mapper_data;

public:
// Constructor with id and the queue to use.
	mapper_spill_checker(shared_mapper_data *mapper_data):m_mapper_data(mapper_data) {

	}

	//check mapper data
	void operator ()() {
		while (true) {

			m_mapper_data->scan();



			// Make sure we can be interrupted
			boost::this_thread::interruption_point();
			boost::this_thread::yield();
		}
	}
};
