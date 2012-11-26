AM_CPPFLAGS=-Wall -I/home/alex/qfs/build/release/include
AM_LDFLAGS= -L/home/alex/qfs/build/release/lib -lboost_program_options -lgearman -lqfs_client -lboost_regex -lboost_thread -lpthread
 
bin_PROGRAMS=qfs_mapred_submit mapper_worker mapper_to_qfs_partitions sorter_worker reducer_worker kvsorter

qfs_mapred_submit_SOURCES=qfs_mapred_submit_main.cc
mapper_worker_SOURCES=mapper_worker_main.cc
mapper_to_qfs_partitions_SOURCES=mapper_to_qfs_partitions_main.cc
sorter_worker_SOURCES=sorter_worker_main.cc
kvsorter_SOURCES=kvsorter_main.cc
reducer_worker_SOURCES=reducer_worker_main.cc
