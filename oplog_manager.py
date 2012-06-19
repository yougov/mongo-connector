"""
The oplog_manager class handles connecting to mongo clusters and getting docs.

An OplogThread is a thread that polls a primary on a single shard and 
gathers all recent updates, and then retrieves the relevant docs and sends it 
to the specified layer above.
"""

from threading import Thread
import pymongo
from util import get_namespace_details
from checkpoint import Checkpoint


class OplogThread(Thread):
    """
    OplogThread gathers the updates for a single oplog. 
    """
    
    def __init__(self, mongo_conn, oplog_coll, is_sharded):
        """
        Initialize the oplog thread.
        """
        Thread.__init__(self)
        self.mongo_connection = mongo_conn
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
        if self.running is True:
            self.running = False
            
    def retrieve_docs(self, oplog_doc_entries):
        """
        Given the doc ID's, retrieve those documents from the mongos.
        """
    
        update_list = {}
      #  timestamp = None
        
        for entry in oplog_doc_entries:
            namespace = entry['ns']
            doc = entry['o']
         #   timestamp = entry['ts']
            operation = entry['op']
            
            if operation == 'i':  #insert
                if self.is_sharded:
                    doc_id = entry['o']['_id']
                    db_name, coll_name = get_namespace_details(namespace)
                    
                    cursor_db = self.mongo_connection[db_name]
                    cursor = cursor_db[coll_name].find({'_id':doc_id})
                    count = cursor.count
                    if count != 0: # it's in mongos => part of a migration
                        continue
                
                #prepare and add sphinx doc
                #update configuration writer
                
            elif operation == 'u': #update
                update_list[namespace] = update_list[namespace] or []
                to_update = update_list[namespace]
                to_update.append(entry['o2']['_id'])
                
            elif operation == 'd': #delete
                update_list[namespace] = update_list[namespace] or []
                to_update = update_list[namespace]
                doc_id = entry['o']['_id']
                
                if self.is_sharded:
                    db_name, coll_name = get_namespace_details(namespace)
                    cursor_db = self.mongo_connection[db_name]
                    cursor = cursor_db[coll_name].find({'_id':id})
                    count = cursor.count
                    
                    if count != 0:
                        continue
                
                to_update.remove(id)
                #prepare and add doc to sphinx
                
            elif operation == 'n':
                pass #do nothing here
            
            for namespace, id_list in update_list:
                database, collection = get_namespace_details(namespace)
                
                to_update = self.mongo_connection.db(database).collection(
                    collection).find({'_id':{'in':id_list}})
                
                for doc in to_update:
                    #prepare and add doc to sphinx
                    pass
                    
                id_list.remove(doc['_id'])
    
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
    