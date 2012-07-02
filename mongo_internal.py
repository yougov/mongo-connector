"""Discovers the mongo cluster and starts the daemon. 
"""

from solr_doc_manager import SolrDocManager
import time
from threading import Thread
from pymongo import Connection
from oplog_manager import OplogThread
        
class Daemon(Thread):
    """Checks the cluster for shards to tail. 
    """
    
    def __init__(self, address, oplog_checkpoint):
        
        super(Daemon, self).__init__()
        self.running = False
        self.oplog_checkpoint = oplog_checkpoint
        self.address = address
        self.setDaemon(True)
        self.shard_set = {}
        
    def stop(self):
        self.running = False
        for thread in self.shard_set:
            thread.stop()

  
    def run(self):
        mongos_conn = Connection(self.address)
        shard_coll = mongos_conn['config']['shards']
        self.running = True
        
        while self.running is True: 
            
            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                
                if self.shard_set.has_key(shard_id):
                    continue

                shard_conn = Connection(shard_doc['host'])
                oplog_coll = shard_conn['local']['oplog.rs']
                
                doc_manager = SolrDocManager('http://127.0.0.1:8080/solr/')
                oplog = OplogThread(shard_conn, self.address, oplog_coll,
                 True, doc_manager, self.oplog_checkpoint, 
                 {'test.test'}).run() #later change run to start
                self.shard_set[shard_id] = oplog
            
       
