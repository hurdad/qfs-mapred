bin_PROGRAMS=qfs_mapred_submit mapper_worker mapper_to_qfs_partitions sorter_worker reducer_worker kvsorter
qfs_mapred_submit_SOURCES=qfs_mapred_submit_main.cc
mapper_worker_SOURCES=mapper_worker_main.cc
mapper_to_qfs_partitions_SOURCES=mapper_to_qfs_partitions_main.cc
sorter_worker_SOURCES=sorter_worker_main.cc
kvsorter_SOURCES=kvsorter_main.cc
reducer_worker_SOURCES=reducer_worker_main.cc
qfs_mapred_submit_LDADD=-lboost_program_options -lgearman -lqfs_client -lqfs_qcdio -lqfs_common -lqfs_qcrs -lqfs_io
mapper_worker_LDADD=-lboost_program_options -lgearman -lqfs_client -lqfs_qcdio -lqfs_common -lqfs_qcrs -lqfs_io
mapper_to_qfs_partitions_LDADD=-lboost_thread -lboost_program_options -lqfs_client -lqfs_qcdio -lqfs_common -lqfs_qcrs -lqfs_io
sorter_worker_LDADD=-lboost_program_options -lgearman -lqfs_client -lqfs_qcdio -lqfs_common -lqfs_qcrs -lqfs_io
reducer_worker_LDADD=-lboost_program_options -lgearman -lqfs_client -lqfs_qcdio -lqfs_common -lqfs_qcrs -lqfs_io
kvsorter_LDADD=-lboost_program_options


