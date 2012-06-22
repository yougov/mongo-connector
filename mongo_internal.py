"""Discovers the mongo cluster and starts the daemon. 
"""

from solr_doc_manager import SolrDocManager
import time

from util import get_oplog_coll, get_connection
from threading import Thread
from pymongo import Connection, ReplicaSetConnection 
from oplog_manager import OplogThread

 
    


class DaemonThread(Thread):
    """ DaemonThread is a wrapper class for the daemon.
    """
    
    
    def __init__(self, address, oplog_checkpoint):
        """Initialize the daemon thread
        """
        Thread.__init__(self)
        self.daemon = Daemon(oplog_checkpoint)
        self.address = address
        self.running = False
        
        
    def run(self):
        """ Start the thread, which runs the daemon. 
        """
        if self.running is False:
            self.running = True 
            self.daemon.run(self.address)


    def stop(self):
        """Stop the thread and daemon (if it's running). 
        """
        if self.running is True:
            self.daemon.stop()
            
        self.running = False
        
        
class Daemon():
    """Checks the cluster for shards to tail. 
    """
    
    def __init__(self, oplog_checkpoint):
        self.running = False
        self.oplog_checkpoint = oplog_checkpoint
        
        
    def stop(self):
        self.running = True
  
  
    def run(self, address):
        """Continuously collect information about the sharded cluster. 
        """
        mongos_conn = get_connection(address)
        shard_set = {}
        shard_coll = mongos_conn['config']['shards']
        self.running = True
        
        while self.running is True: 
            
            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                
                if shard_set.has_key(shard_id):
                    continue
    
                shard_conn = Connection(shard_doc['host'])
                stat = shard_conn['admin'].command({'replSetGetStatus':1})
                repl_set = stat['set']
                shard_conn = ReplicaSetConnection(shard_doc['host'],
                    replicaSet = repl_set)
                
                oplog_coll = get_oplog_coll(shard_conn, 'repl_set')
                
                doc_manager = SolrDocManager('http://127.0.0.1:8080/solr/')
                oplog = OplogThread(shard_conn, address, oplog_coll,
                 True, doc_manager, self.oplog_checkpoint, 
                 {'test.test'}).start() 
                shard_set[shard_id] = oplog
            
            print 'sleeping for a bit now...' 
            time.sleep(5) # for testing purposes 
            
       