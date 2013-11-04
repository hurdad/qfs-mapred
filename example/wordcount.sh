#!/bin/sh

timestamp() {
  date +"%F %T"
}

if [ -z "$BASEPATH" ];
 then echo "Missing BASEPATH";
 exit
fi

#path vars
QFS_LIB=$BASEPATH/qfs/build/release/lib
QFS_TOOLS_BIN=$BASEPATH/qfs/build/release/bin/tools/
QFS_MAPRED_BIN=$BASEPATH/qfs-mapred/bin/

#connection vars
QFS_METASERVER_HOST=127.0.0.1
QFS_METASERVER_PORT=20000
GEARMAND_HOST=127.0.0.1
GEARMAND_PORT=5000

#change to script directory so relative paths below work
cd "$(dirname "$0")"

echo "$(timestamp): Loading data into qfs:/wordcount"
#load data into qfs with Stripe enabled
$QFS_TOOLS_BIN/qfsshell -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -q -- rm /wordcount/
$QFS_TOOLS_BIN/qfsshell -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -q -- mkdir /wordcount/input/
$QFS_TOOLS_BIN/qfsshell -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -q -- mkdir /wordcount/map/
$QFS_TOOLS_BIN/qfsshell -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -q -- mkdir /wordcount/output/
$QFS_TOOLS_BIN/cptoqfs -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -d ../data/pg4300.txt -k /wordcount/input/ -S
$QFS_TOOLS_BIN/cptoqfs -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT -d ../data/pg5000.txt -k /wordcount/input/ -S

#add log dir if doent eist
[ -d $QFS_MAPRED_BIN/log ] || mkdir $QFS_MAPRED_BIN/log

#start workers
echo "$(timestamp): Starting workers in background"
LD_LIBRARY_PATH=$QFS_LIB ${QFS_MAPRED_BIN}/mapper_worker -h $GEARMAND_HOST -p $GEARMAND_PORT --timeout -1 --path_to_qfs_bin_tools $QFS_TOOLS_BIN --path_to_qfs_mapred_bin $QFS_MAPRED_BIN > $QFS_MAPRED_BIN/log/mapper.log 2>&1 &
MAPPER_PID=$!
echo 'Reducer Worker PID: '$!
LD_LIBRARY_PATH=$QFS_LIB ${QFS_MAPRED_BIN}/sorter_worker -h $GEARMAND_HOST -p $GEARMAND_PORT --timeout -1 --path_to_qfs_bin_tools $QFS_TOOLS_BIN --path_to_qfs_mapred_bin $QFS_MAPRED_BIN > $QFS_MAPRED_BIN/log/sorter.log 2>&1 &
SORTER_PID=$!
echo 'Sorter Worker PID: '$!
LD_LIBRARY_PATH=$QFS_LIB ${QFS_MAPRED_BIN}/reducer_worker -h $GEARMAND_HOST -p $GEARMAND_PORT --timeout -1 --path_to_qfs_bin_tools $QFS_TOOLS_BIN > $QFS_MAPRED_BIN/log/reducer.log 2>&1 &
REDUCER_PID=$!
echo 'Reducer Worker PID: '$!

#submit job
echo "$(timestamp): Submitting MapReduce Job"
LD_LIBRARY_PATH=$QFS_LIB $QFS_MAPRED_BIN/qfs_mapred_submit --gearmand_host $GEARMAND_HOST --gearmand_port $GEARMAND_PORT --meta_server_host $QFS_METASERVER_HOST --meta_server_port $QFS_METASERVER_PORT --qfs_input_folder /wordcount/input/ --qfs_map_folder /wordcount/map/ --qfs_output_folder /wordcount/output/ --mapper $BASEPATH/qfs-mapred/scripts/wordcount-mapper.py --reducer $BASEPATH/qfs-mapred/scripts/wordcount-reducer.py

#copy results from qfs
echo "$(timestamp): Getting results from qfs:/wordcount"
$QFS_TOOLS_BIN/qfsshell -s localhost -p 20000 -q -- ls wordcount/output > FILES
sed 's/^/\/wordcount\/output\//g' FILES > FILES2
sed ':a;N;$!ba;s/\n/ /g' FILES2 > FILES3
FILES=`cat FILES3`

#concat output
echo "$(timestamp): Concatenating results to ${PWD}/wc_output.txt"
LD_LIBRARY_PATH=$QFS_LIB $QFS_TOOLS_BIN/qfscat -s $QFS_METASERVER_HOST -p $QFS_METASERVER_PORT $FILES > wc_output.txt

#kill workers
echo "$(timestamp): Killing Mapper Worker PID: $MAPPER_PID"
kill $MAPPER_PID
echo "$(timestamp): Killing Sorder Worker PID: $SORTER_PID"
kill $SORTER_PID
echo "$(timestamp): Killing Mapper Worker PID: $REDUCER_PID"
kill $REDUCER_PID

