"""
The oplog_manager class handles connecting to mongo clusters and getting docs.

An OplogThread is a thread that polls a primary on a single shard and 
gathers all recent updates, and then retrieves the relevant docs and sends it 
to the specified layer above.
"""

from threading import Thread
import pymongo
from util import get_namespace_details, get_connection
from checkpoint import Checkpoint


class OplogThread(Thread):
    """
    OplogThread gathers the updates for a single oplog. 
    """
    
    def __init__(self, primary_conn, mongos_address, oplog_coll, is_sharded):
        """
        Initialize the oplog thread.
        """
        Thread.__init__(self)
        self.primary_connection = primary_conn
        self.mongos_address = mongos_address
        self.oplog = oplog_coll
        self.is_sharded = is_sharded
        self.running = False
        self.checkpoint = None
        
    def run(self):
        """
        Start the oplog worker.
        """
        self.running = True  
        
        if self.is_sharded is False:
            print 'handle later'
            return
              
        while self.running is True:    
            
            cursor = self.init_sync()
            oplog_doc_batch = []
            #oplog_doc_count = 0
            
            for doc in cursor:
                if doc is None:
                    break
                
                oplog_doc_batch.append(doc)
               # oplog_doc_count += 1
            
                last_timestamp = doc['ts']
                self.checkpoint.commit_ts = last_timestamp
            
            self.retrieve_docs(oplog_doc_batch)
    
    
    def stop(self):
        """
        Stop this thread from managing the oplog.
        """
        self.running = False
            
            
    def retrieve_docs(self, oplog_doc_entries):
        """
        Given the doc ID's, retrieve those documents from the mongos.
        """
        
        #boot up new mongos_connection for this thread
        mongos_connection = get_connection(self.mongos_address)
        doc_map = {}
        update_list = {}
        
        for entry in oplog_doc_entries:
            namespace = entry['ns']
            doc = entry['o']
            operation = entry['op']
            
            if operation == 'i':  #insert
            
                #from a migration, so ignore for now
                if entry.has_key('fromMigrate'):
                    continue
                    
                doc_id = entry['o']['_id']
                db_name, coll_name = get_namespace_details(namespace)
                doc = mongos_connection[db_name][coll_name].find_one({'_id':doc_id})
                doc_map[doc_id] = namespace
                
            elif operation == 'u': #update
                doc_id = entry['o']['_id']
                doc_map[doc_id] = namespace
                
            elif operation == 'd': #delete
                #from a migration, so ignore for now
                if entry.has_key('fromMigrate'):
                     continue
               # update_list[namespace] = update_list[namespace] or []
               # to_update = update_list[namespace]
                doc_id = entry['o']['_id']
                #need more logic here
                
            elif operation == 'n':
                pass #do nothing here
            
            for doc_id, namespace in doc_map:
                db_name, coll_name = get_namespace_details(namespace)
                doc = mongos_connection[db_name][coll_name].find_one({'_id':doc_id})
                doc_map[doc_id] = doc
                
            #at this point, doc_map has a full doc for every doc_id updated/inserted
    
    def get_oplog_cursor(self, timestamp):
        """
        Returns the cursor to the place in the oplog just after the last recorded 
        timestamp 
        """
        ret = None
        
        if timestamp is not None:
            cursor = self.oplog.find(spec={'op':{'$ne':'n'}, 
            'ts':{'$gte':0}}, tailable=True, order={'$natural':'asc'})
            doc = cursor.next_document     
            
        #means we didn't get anything from the cursor, or no timestamp 
        if doc is None or cursor.__getitem__() is None:
            entry = self.oplog.find_one(spec={'ts':timestamp})
            
            if entry is None:
                sub_doc = self.oplog.find_one(
                spec={'ts':{'$lt':timestamp}})      
            if sub_doc is None:
                pass #uh oh - rollback
            
            ret = cursor
            
        elif timestamp == doc['ts']:
            ret = cursor
        
        return ret
        
    def get_last_oplog_timestamp(self):
        """
        Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find(sort={'$natural':'desc'}, limit=1)
        return curr.__getitem__('ts')
            
            
    def full_dump(self):
        """
        Dumps the database, returns the cursor to the latest entry just before the dump
        is performed.
        """
        timestamp = self.get_last_oplog_timestamp()
        self.checkpoint = Checkpoint()
        self.checkpoint.commit_ts = timestamp
        cursor = self.get_oplog_cursor(timestamp)
        
        return cursor
        
    def init_sync(self):
        """ 
        Initializes the cursor for the sync method. 
        """
        
        cursor = None
        last_commit = None
        
        if self.checkpoint is None:
            cursor = self.full_dump()
        else:
          #  self.restore_checkpoint(self.checkpoint)
            last_commit = self.checkpoint.commit_ts
            
            cursor = self.get_oplog_cursor(last_commit)
            if cursor is None:
                cursor = self.full_dump()
                
        return cursor
    