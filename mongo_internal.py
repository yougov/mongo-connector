"""
mongo_internal.py contains all the code that discovers the mongo cluster
and starts the worker threads. 
"""

from util import get_oplog_coll, upgrade_to_replset
from threading import Thread
from pymongo import Connection, ReplicaSetConnection 
from oplog_manager import OplogThread

 
    


class DaemonThread(Thread):
    """
    DaemonThread is a wrapper class for the daemon.
    """
    
    
    def __init__(self, host, address):
        """
        Initialize the daemon thread
        """
        Thread.__init__(self)
        self.daemon = Daemon()
        self.host = host
        self.address = address
        self.running = False
        
        
    def run(self):
        """ 
        Start the thread, run the daemon. 
        """
        if self.running is False:
            self.running = True 
            mongos_conn = prepare_daemon_args(self.host)
            self.daemon.run(mongos_conn)


    def stop(self):
        """
        Stop the thread and daemon (if it's running). 
        """
        if self.running is True:
            self.daemon.stop()
            
        self.running = False
        
        
class Daemon():
    """
    The Daemon class has the main run method which runs forever and
    gathers documents that have been updated for the synchronizer.
    """
    
    def __init__(self):
        """
        Initialize the Daemon.
        """
        self.running = False
        
        
    def stop(self):
        """
        Stop the Daemon.
        """
        self.running = True
  
  
    def run(self, mongos_conn):
        """
        Continuously collect doc information from a sharded cluster. 
        """
        shard_set = {}
        shard_coll = mongos_conn['config']['shards']
        self.running = True
        
        while self.running is True: 
            
            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                host = shard_doc['host'].split(',')[0]
                
                if shard_set.has_key(shard_id):
                    continue
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
                
                oplog = OplogThread(shard_conn, oplog_coll, True).start() 
                shard_set[shard_id] = oplog
            
            
def prepare_daemon_args(address):
    """
    For now, it takes in an address and returns the mongos connection to it.
    
    Needs error checking. 
    """
    host, port = address.split(':')
    conn = Connection(host, int(port))
    return conn
       