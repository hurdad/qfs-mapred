#!/bin/sh

timestamp() {
  date +"%F %T"
}

if [ -z "$BASEPATH" ];
 then echo "Missing BASEPATH";
 exit
fi

#path vars
QFS_LIBS=$BASEPATH/qfs/build/release/libs
QFS_TOOLS_BIN=$BASEPATH/qfs/build/release/bin/tools
QFS_MAPRED_BIN=$BASEPATH/qfs-mapred/bin

#connection vars
QFS_METASERVER_HOST=localhost
QFS_METASERVER_PORT=20000
GEARMAND_HOST=localhost
GEARMAND_PORT=4730

#change to script directory so relative paths below work
cd "$(dirname "$0")"

#set LD_LIBRARY_PATH to qfs libs
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH":$QFS_LIBS

echo "$(timestamp): Loading data into qfs:/wordcount"
#load data into qfs with Stripe enabled
$QFS_TOOLS_BIN/qfsshell -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -q -- mkdir /wordcount
$QFS_TOOLS_BIN/cptoqfs -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -d ../data/pg4300.txt -k /wordcount/ -S

#add log dir if doent eist
[ -d $QFS_MAPRED_BIN/log ] || mkdir $QFS_MAPRED_BIN/log

echo "$(timestamp): Starting workers in background"
#start workers
$QFS_MAPRED_BIN/mapper_worker -h $GEARMAND_HOST -p $GEARMAND_PORT --path_to_qfs_bin_tools $QFS_TOOLS_BIN --path_to_qfs_mapred_bin $QFS_MAPRED_BIN > $QFS_MAPRED_BIN/log/mapper.log 2>&1
$QFS_MAPRED_BIN/sorter_worker -h $GEARMAND_HOST -p $GEARMAND_PORT --path_to_qfs_bin_tools $QFS_TOOLS_BIN --path_to_qfs_mapred_bin $QFS_MAPRED_BIN > $QFS_MAPRED_BIN/log/sorter.log 2>&1
$QFS_MAPRED_BIN/reducer_worker -h $GEARMAND_HOST -p $GEARMAND_PORT --path_to_qfs_bin_tools $QFS_TOOLS_BIN > $QFS_MAPRED_BIN/log/reducer.log 2>&1

echo "$(timestamp): Submitting Wordcount Job"
#submit job
#$QFS_MAPRED_BIN/qfs_mapred_submit

echo "$(timestamp): Getting results from qfs:/wordcount"
#copy results from qfs

