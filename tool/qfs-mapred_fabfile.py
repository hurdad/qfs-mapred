#!/usr/bin/env python
# Fabric file for qfs-mapred workers
# Author: Alex Hurd
# Requires:
#    - redis-server
#    - python-redis
#    - fabric
# Usage: fab -f qfs-mapred_fabfile.py --hosts=host1,host2 start:mapper_worker

from fabric.api import env, run, sudo
import redis

env.password = 'raspberry'
bin_path = "/opt/hurdad/qfs-mapred/bin/"
log_path = "/opt/hurdad/qfs-mapred/log/"
qfs_tool_bin_path = "/opt/qc/qfs/client/bin/"
qfs_client_lib_path = "/opt/qc/qfs/client/lib/"
worker_user = 'mapred'

redis_host = 'localhost'
redis_port = 6379

gearman_job_server_hostname = "10.10.0.14"
gearman_job_server_port = 4730

def start_all():

	start('mapper_worker')
	start('sorter_worker')
	start('reducer_worker')

def stop_all():

	stop('mapper_worker')
	stop('sorter_worker')
	stop('reducer_worker')

def status_all():

	status('mapper_worker')
	status('sorter_worker')
	status('reducer_worker')

def start(daemon):
    
    if daemon == 'mapper_worker':
        pid = sudo("LD_LIBRARY_PATH=%s %smapper_worker -h %s -p %s --path_to_qfs_bin_tools %s --path_to_qfs_mapred_bin %s > %s/mapper.log 2>&1 &\necho $!" % 
                (qfs_client_lib_path, bin_path, gearman_job_server_hostname, gearman_job_server_port, qfs_tool_bin_path, bin_path, log_path), pty=False, user=worker_user)
    elif daemon == 'sorter_worker':
        pid = sudo("LD_LIBRARY_PATH=%s %ssorter_worker -h %s -p %s --path_to_qfs_bin_tools %s --path_to_qfs_mapred_bin %s > %s/sorter.log 2>&1 &\necho $!" % 
                (qfs_client_lib_path, bin_path, gearman_job_server_hostname, gearman_job_server_port, qfs_tool_bin_path, bin_path, log_path), pty=False, user=worker_user)
    elif daemon == 'reducer_worker':
        pid = sudo("LD_LIBRARY_PATH=%s %sreducer_worker -h %s -p %s --path_to_qfs_bin_tools %s > %s/reducer.log 2>&1 &\necho $!" % 
                (qfs_client_lib_path, bin_path, gearman_job_server_hostname, gearman_job_server_port, qfs_tool_bin_path, log_path), pty=False, user=worker_user)
    else:
        print 'invalid worker type : mapper_worker | sorter_worker | reducer_worker'
        return
    
    # save
    r = redis.Redis(redis_host, redis_port, db=0)
    r.set('qfs-mapred:%s:%s' % (daemon, env.host) , pid)
 
def stop(daemon):
    r = redis.Redis(redis_host, redis_port, db=0)
    keys = r.keys('qfs-mapred:%s:%s' % (daemon, env.host))
    if keys:
        pid = r.get(keys[0])
	if pid:
       	    sudo("kill %s" % pid)
            r.delete(keys[0])

def status(daemon):
    r = redis.Redis(redis_host, redis_port, db=0)
    keys = r.keys('qfs-mapred:%s:%s' % (daemon, env.host))
    if keys:
        pid = r.get(keys[0])
	if pid:
            ret = ps(pid)
    
def ps(pid):
    """Get process info for the `daemon`."""
    run("ps --pid %s" % pid)
