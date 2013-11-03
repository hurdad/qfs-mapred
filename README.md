Overview
=============

qfs_mapred is a simple streaming map reduce implementation using qfs and gearman.

Components
-------
Qfs – Quantcast File System : Distributed, fault tolerant file system based on GFS, written in C++. Swap out replacement for Hadoop's HDFS.

Gearman – Job queue framework used to send generic task to workers. Simple framework for forking application code to other computers. 

Implementation of Map Reduce Architecture
-------

The approach was to keep the implementation as simple as possible, therefor streaming python is used for the user provided map and reduce functions. This allows for easy debugging and portability.

 Here are the 3 steps:

### Mapping:

Input to a map reduce job is a folder of input text files that is stored in qfs. Each file is read into a mapper worker, so there is not automatic splitting of data. Simply split up the input files as needed. Ideally you would have one file per mapper worker slot, larger the file size the better. The output of the mapper goes back into qfs into a set of partitioned files. Since multiple mappers may be streaming data to the same output partitions, qfs has a special record appender that guarantees that atomic writes in the qfs.

Simplified mapper worker system command:
    `cpfromqfs -k /input0 | mapper.py | mapper_to_qfs_partitions`

mapper_to_qfs_partitions tool also allows for bulk record appending by spilling data for each partition after  a specified amount of  bytes have been produced by the mappers. 

### Sorting:

The map reduce framework requires the intermediate data, mapper output, to be sorted by key for the reducers. The implementation of this step is very different that Hadoop's shuffle/sort algorithm.  Each partition is read by a sorter worker from qfs into system memory, then sorted and write back to qfs. This has several trade offs, first the size of a partition cannot be larger that the system memory or system pageing will occur (swapping). However, the implementation is very simple, also both reading & writing large chunks from qfs is efficient which will improve the sort's comparison times because the  sorting occurs in memory, not in qfs.
Simplified sorter  worker system command: 
    `cpfromqfs -k /mapper_out | kvsorter | cptoqfs -k /mapper_out_sorted`


### Reducing:
Each reducer worker is sent the file list of the sorted intermediate that is ready for reducing. The output of each reducer is sent back to qfs into its own file. Once all reducers are finished the outputs can be concatenated back into  qfs to produce an unsorted result of the map reduce job.

Simplifed reducer worker system comand: 
    `cpfromqfs -k /mapper_out_sorted | reducer.py | cptoqfs -k /output0`


Getting Started
---------------------

### Requirements:

* qfs cluster installation as well as qfs binary tools (cptoqfs/cpfromqfs/qfscat)
* gearman job server, typically to be run on the qfs meta server.
* Python CLI

### Shared Library Requirements:
* libqfs_client
* libboost_regex (qfs dependency)
* libboost_program_options
* libboost_thread
* libgearman

Quick Start
----------------------

