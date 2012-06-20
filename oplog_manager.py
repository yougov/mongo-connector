"""
The oplog_manager class handles connecting to mongo clusters and getting docs.

An OplogThread is a thread that polls a primary on a single shard and 
gathers all recent updates, and then retrieves the relevant docs and sends it 
to the specified layer above.
"""

import pymongo
import time
import os
import simplejson as json
from bson.objectid import ObjectId
from threading import Thread
from util import get_namespace_details, get_connection, get_next_document
from checkpoint import Checkpoint
from doc_manager import DocManager


class OplogThread(Thread):
    """
    OplogThread gathers the updates for a single oplog. 
    """
    
    def __init__(self, primary_conn, mongos_address, oplog_coll, is_sharded, doc_manager):
        """
        Initialize the oplog thread.
        """
        Thread.__init__(self)
        self.primary_connection = primary_conn
        self.mongos_address = mongos_address
        self.oplog = oplog_coll
        self.is_sharded = is_sharded
        self.doc_manager = doc_manager
        self.running = False
        self.checkpoint = None
        self.config = None
        
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

            for doc in cursor:
                
                oplog_doc_batch.append(doc)
            
                last_timestamp = doc['ts']
                self.checkpoint.commit_ts = last_timestamp
            
            self.retrieve_docs(oplog_doc_batch)

            time.sleep(2)   
            
    
    
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

                doc_id = entry['o']['_id']
                #need more logic here
                
            elif operation == 'n':
                pass #do nothing here
                       
        for doc_id in doc_map:
        
            #done to avoid issue with iterating over ObjectId's
            namespace = doc_map[doc_id] 
            
            db_name, coll_name = get_namespace_details(namespace)
            doc = mongos_connection[db_name][coll_name].find_one({'_id':doc_id})
            self.doc_manager.add_doc(doc_id, doc)
        
        doc_map.clear()
                
            #at this point, all docs are added to the queue
    
    def get_oplog_cursor(self, timestamp):
        """
        Returns the cursor to the place in the oplog just after the last recorded 
        timestamp 
        """
        ret = None
        
        if timestamp is not None:
            cursor = self.oplog.find(spec={'op':{'$ne':'n'}, 
            'ts':{'$gte':timestamp}}, tailable=True, order={'$natural':'asc'})
            doc = get_next_document(cursor)     
            ret = cursor
            
        #means we didn't get anything from the cursor, or no timestamp
        """ 
        if doc is None or cursor[0] is None:
            print 'in get_oplog_cursor, doc is None or cursor[0] is None'
            entry = self.oplog.find_one(spec={'ts':timestamp})
            
            if entry is None:
                sub_doc = self.oplog.find_one(
                spec={'ts':{'$lt':timestamp}})      
            if sub_doc is None:
                pass #uh oh - rollback
            
            ret = cursor
            
        elif timestamp == doc['ts']:
            ret = cursor
        """
        
        return ret
        
    def get_last_oplog_timestamp(self):
        """
        Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural',pymongo.DESCENDING).limit(1)
        return curr[0]['ts']
        
        
    def get_first_oplog_timestamp(self):
        """
        Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural',pymongo.ASCENDING).limit(1)
        return curr[0]['ts']
        
    def init_cursor(self):
        """
        Position the cursor to the beginning of the oplog.
        """
        timestamp = self.get_first_oplog_timestamp()
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
        
        if self.config is not None:
            pass        # do stuff here 
        
        if self.checkpoint is None:
            cursor = self.init_cursor()
        else:
          #  self.restore_checkpoint(self.checkpoint)
            last_commit = self.checkpoint.commit_ts
            
            cursor = self.get_oplog_cursor(last_commit)
            if cursor is None:
                cursor = self.init_cursor()
                
        return cursor
        
    def write_config(self):
        """
        Write the updated config to the config file. 
        
        This is done by duplicating the old config file, editing the relevant
        timestamp, and then copying the new config onto the old file. 
        """
        os.rename(self.config, self.config+'~')  # '~' indicates temp file
        dest = open(self.config, 'w')
        source = open(self.config+'~', 'r')
        oplog_str = str(self.oplog.database.connection)
        json_str = json.dumps([oplog_str, self.checkpoint.commit_ts])
            
        for line in source:
            if oplog_str in line:
                dest.write(json_str)        # write updated timestamp
            else:
                dest.write(line)
        
        source.close()
        dest.close()
        os.remove(self.config+'~')
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    