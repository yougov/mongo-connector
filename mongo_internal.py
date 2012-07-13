"""Discovers the mongo cluster and starts the daemon. 
"""

from doc_manager import DocManager
import time
from threading import Thread
from pymongo import Connection
from oplog_manager import OplogThread
        

class Daemon(Thread):
    """Checks the cluster for shards to tail. 
    """
    
    def __init__(self, address, oplog_checkpoint, backend_url, ns_set, u_key):
        super(Daemon, self).__init__()
        self.canRun = True
        self.oplog_checkpoint = oplog_checkpoint
        self.address = address
        self.backend_url = backend_url
        self.ns_set = ns_set
        self.u_key = u_key
        #self.setDaemon(True)
        self.shard_set = {}
        
    def stop(self):
        self.canRun = False


  
    def run(self):
        """Discovers the mongo cluster and creates an oplog thread for each thread 
        """
        mongos_conn = Connection(self.address)
        shard_coll = mongos_conn['config']['shards']
        doc_manager = DocManager(self.backend_url)
        if doc_manager is None:
            print 'Bad backend URL!'
            return
        
        while self.canRun is True: 
            
            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                if self.shard_set.has_key(shard_id):
                    time.sleep(2)
                    continue
                    
                shard_conn = Connection(shard_doc['host'])
                oplog_coll = shard_conn['local']['oplog.rs']
                oplog = OplogThread(shard_conn, self.address, oplog_coll,
                 True, doc_manager, self.oplog_checkpoint, self.ns_set)
                self.shard_set[shard_id] = oplog
                oplog.start()
          
        #time to stop running
        for thread in self.shard_set.values():
            thread.stop()      
            
       
