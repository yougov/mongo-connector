"""
mongo_internal.py contains all the code that connects to the mongo cluster, gets status of
updated documents. 
"""

from util import get_oplog_coll, upgrade_to_replset, verify_url
from threading import Thread
from pymongo import Connection, ReplicaSetConnection 


class DaemonThread(Thread):
    """
    DaemonThread is a wrapper class for the daemon that coordinates
    reading oplogs and storing them.
    """
    
    def __init__(self, daemon, host):
        """
        Initialize the daemon thread
        """
    
        Thread.__init__(self)
        self.daemon = daemon
        self.host = host
        self.running = False
        
    def start(self, *args):
        """ 
        Start the thread, run the daemon. 
        """
    
        if self.running is False:
            self.running = True 
            self.daemon.run(*args)

    def stop(self):
        """
        Stop the thread and daemon (if it's running). 
        """
        
        if self.running is True:
            self.daemon.stop()
            
        self.thread = None
        
        
class Daemon():
    """
    The Daemon class has the main run method which runs forever and
    gathers documents that have been updated for the synchronizer.
    """
    
    def __init__(self):
        """
        Initialize the Daemon.
        """
        self.stop = False
        
    def __stop__(self):
        """
        Stop the Daemon.
        """
        self.stop = True
        
    
        
    def run(self, mongo_connection, oplog_collection, is_sharded):
        """
        Connect to the external service, and send information. 
        """
        
        sync_set = {}
    
        while self.stop is False:
            new_sync_set = {}

            #we will assume that we are passed the host/port for sphinx servers
            url = "127.0.0.1:9312"

            new_ns_set = {}
            host, port = url.split(":")
            new_ns_set[host] = "port"

            if sphinx_sync_set.has_key(url):
                sphinx_sync = sphinx_sync_set[url]
                sphinx_sync.update_config({'ns_set':new_ns_set})
                
                del sphinx_sync_set[url]  
            elif verify_url(url):
                sphinx = SphinxClient()
                sphinx.SetServer(host, port)

                sync = SphinxSynchronizer(sphinx, mongo_connection, 
                    oplog_collection, None, new_ns_set, is_sharded)
                sphinx_sync = SphinxSyncThread(sync)
                sphinx_sync.start()
            else:
                sphinx_sync = None

            if sphinx_sync is not None:
                new_sphinx_sync_set[url] = sphinx_sync
                    
            #clear out the old config threads    
            for url, sphinx_thread in sphinx_sync_set:
                sphinx_thread.stop()
                
            sphinx_sync_set = new_sphinx_sync_set
            
  
    def run_w_shard(self, mongo_conn):
        """
        Continuously collect doc information from a sharded cluster. 
        """
        
        shard_set = {}
        shard_coll = mongo_conn['config']['shards']
        
        while self.stop is False: 
            new_shard_set = {}
            
            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                host = shard_doc['host'].split(',')[0]
                
                if shard_set.has_key(shard_id):
                    shard = shard_set[shard_id]
                    del shard_set[shard_id]
                elif '/' in host:
                    address = host.split('/')[0]
                else: # not a replica set
                    address = host
                    
                location, port = address.split(':')
                shard_conn = Connection(location, int(port))
                shard_conn = upgrade_to_replset(shard_conn)
                
                if isinstance(shard_conn, ReplicaSetConnection):
                    oplog_coll = get_oplog_coll(shard_conn, 'repl_set')
                else:
                    oplog_coll = get_oplog_coll(shard_conn, 'master_slave')
                
                DaemonThread(Daemon(), host).start(
                    mongo_conn, oplog_coll, True) 
                new_shard_set[shard_id] = shard
                
            for daemon_thread in shard_set.values():
                daemon_thread.stop()
                
            shard_set = new_shard_set
       