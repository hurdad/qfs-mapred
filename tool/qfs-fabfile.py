#!/usr/bin/env python

# Fabric file for qfs clusters
# Author: Alex Hurd
# Requires:
#    - redis-server
#    - python-redis
#    - fabric
# Usage: fab -f qfs_fabfile.py --hosts=host1,host2 start:chunkserver

from fabric.api import env, run, sudo
import redis

env.password = 'raspberry'
qfs_base_path = "/opt/qc/qfs/"
daemon_user = 'qfs'
redis_host = 'localhost'
redis_port = 6379
qfs_client_lib_path = "/opt/qc/qfs/client/lib/"
qfs_client_python_path = "/opt/qc/qfs/client/python/"
def start(daemon):
    
    if daemon == 'chunkserver':
        pid = sudo("%schunkserver/bin/chunkserver %schunkserver/conf/ChunkServer.prp %schunkserver/ChunkServer.log > %schunkserver/chunkserver.out 2>&1 &\necho $!" % 
                (qfs_base_path, qfs_base_path, qfs_base_path, qfs_base_path), pty=False, user=daemon_user)
    elif daemon == 'metaserver':
        pid = sudo("%smetaserver/bin/metaserver -c %smetaserver/conf/MetaServer.prp %smetaserver/MetaServer.log > %smetaserver/metaserver.out 2>&1 &\necho $!" % 
                (qfs_base_path, qfs_base_path, qfs_base_path, qfs_base_path), pty=False, user=daemon_user)
    elif daemon == 'webui':
        pid = sudo("LD_LIBRARY_PATH=%s PYTHONPATH=%s python %smetaserver/webui/qfsstatus.py %smetaserver/webui/server.conf > %smetaserver/webui/webui.log 2>&1 &\necho $!" % 
                (qfs_client_lib_path, qfs_client_python_path, qfs_base_path, qfs_base_path, qfs_base_path), pty=False, user=daemon_user)
    else:
        print 'invalid daemon type : chunkserver | metaserver | webui'
        return
    
    # save
    r = redis.Redis(redis_host, redis_port, db=0)
    r.set('qfs:%s:%s' % (daemon, env.host) , pid)
 
def stop(daemon):
    r = redis.Redis(redis_host, redis_port, db=0)
    key = r.keys('qfs:%s:%s' % (daemon, env.host))
    if key:
        pid = r.get(key[0])
        sudo("kill %s" % pid)
        r.delete(key)

def status(daemon):
    r = redis.Redis(redis_host, redis_port, db=0)
    key = r.keys('qfs:%s:%s' % (daemon, env.host))
    if key:
        pid = r.get(key[0])
        ret = ps(pid)
    
def ps(pid):
    """Get process info for the `daemon`."""
    run("ps --pid %s" % pid)

