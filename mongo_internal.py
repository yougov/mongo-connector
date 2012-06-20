"""
mongo_internal.py contains all the code that discovers the mongo cluster
and starts the worker threads. 
"""

from util import get_oplog_coll, upgrade_to_replset, get_connection
import doc_manager
import time
from threading import Thread
from pymongo import Connection, ReplicaSetConnection 
from oplog_manager import OplogThread

 
    


class DaemonThread(Thread):
    """
    DaemonThread is a wrapper class for the daemon.
    """
    
    
    def __init__(self, address, doc_man):
        """
        Initialize the daemon thread
        """
        Thread.__init__(self)
        self.daemon = Daemon(doc_man)
        self.address = address
        self.running = False
        
        
    def run(self):
        """ 
        Start the thread, run the daemon. 
        """
        if self.running is False:
            self.running = True 
            self.daemon.run(self.address)


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
    
    def __init__(self, doc_man):
        """
        Initialize the Daemon.
        """
        self.running = False
        self.doc_manager = doc_man
        
        
    def stop(self):
        """
        Stop the Daemon.
        """
        self.running = True
  
  
    def run(self, address):
        """
        Continuously collect doc information from a sharded cluster. 
        """
        mongos_conn = get_connection(address)
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
                    address = host.split('/')[1]
                else: # not a replica set
                    address = host
                    
                print 'address = ' + address
                    
                location, port = address.split(':')
                shard_conn = Connection(location, int(port))
                shard_conn = upgrade_to_replset(shard_conn)
                
                if isinstance(shard_conn, ReplicaSetConnection):
                    oplog_coll = get_oplog_coll(shard_conn, 'repl_set')
                else:
                    oplog_coll = get_oplog_coll(shard_conn, 'master_slave')
                
                oplog = OplogThread(shard_conn, address, oplog_coll,
                 True, self.doc_manager).start() 
                shard_set[shard_id] = oplog
            
            print 'sleeping for a bit now...'
            time.sleep(5)
            
       