#!/bin/sh

if [ -z "$BASEPATH" ];
 then echo "Missing BASEPATH";
fi

#path vars
$QFS_LIBS=$BASEPATH/qfs/build/release/libs
$QFS_TOOLS_BIN=$BASEPATH/qfs/build/release/bin
$QFS_MAPRED_BIN=$BASEPATH/qfs-mapred/bin

#change to script directory so relative paths below work
cd "$(dirname "$0")"

#set LD_LIBRARY_PATH to qfs libs
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH":$QFS_LIBS

#load data into qfs with Stripe enabled
$QFS_TOOLS_BIN/cptoqfs -s localhost -p 30000 -d ../data/pg4300.txt -k /wordcount -S

#add log dir if doent eist
[ -d $QFS_MAPRED_BIN/log ] || mkdir $QFS_MAPRED_BIN/log

#start workers
$QFS_MAPRED_BIN/mapper_worker -h localhost -p 4730 --path_to_qfs_bin_tools $QFS_TOOLS_BIN --path_to_qfs_mapred_bin $QFS_MAPRED_BIN > $QFS_MAPRED_BIN/log/mapper.log 2>&1
$QFS_MAPRED_BIN/sorter_worker -h localhost -p 4730 --path_to_qfs_bin_tools $QFS_TOOLS_BIN --path_to_qfs_mapred_bin $QFS_MAPRED_BIN > $QFS_MAPRED_BIN/log/sorter.log 2>&1
$QFS_MAPRED_BIN/reducer_worker -h localhost -p 4730 --path_to_qfs_bin_tools $QFS_TOOLS_BIN > $QFS_MAPRED_BIN/log/reducer.log 2>&1

#submit job
$QFS_MAPRED_BIN/qfs_mapred_submit

#copy reslts from qfs
