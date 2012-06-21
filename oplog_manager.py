"""Tails the oplog of a shard and returns entries"""

import pymongo
import time
import os
import json

from bson.objectid import ObjectId
from threading import Thread
from util import (get_namespace_details,
                  get_connection, 
                  get_next_document)
from checkpoint import Checkpoint
from doc_manager import DocManager


class OplogThread(Thread):
    """OplogThread gathers the updates for a single oplog. 
    """
    
    def __init__(self, primary_conn, mongos_address, oplog_coll, is_sharded,
     doc_manager, oplog_file):
        """Initialize the oplog thread.
        """
        Thread.__init__(self)
        self.primary_connection = primary_conn
        self.mongos_address = mongos_address
        self.oplog = oplog_coll
        self.is_sharded = is_sharded
        self.doc_manager = doc_manager
        self.running = False
        self.checkpoint = None
        self.oplog_doc_batch = []
        self.oplog_file = oplog_file
        
    def run(self):
        """Start the oplog worker.
        """
        self.running = True  
        
        if self.is_sharded is False:
            print 'handle later'
            return
              
        while self.running is True:    
            
            cursor = self.prepare_for_sync()
            last_ts = None
            self.retrieve_docs()
            
            for doc in cursor:
                self.oplog_doc_batch.append(doc)
                last_ts = doc['ts']
            
            self.checkpoint.commit_ts = last_ts
            self.write_config()

            time.sleep(2)   #for testing purposes
            
    
    
    def stop(self):
        """Stop this thread from managing the oplog.
        """
        self.running = False
            
            
    def retrieve_docs(self):
        """Given the doc ID's, retrieve those documents from the mongos.
        """
        #boot up new mongos_connection for this thread
        mongos_connection = get_connection(self.mongos_address)
        doc_map = {}
        oplog_batch_size = len(self.oplog_doc_batch)
        
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
        
        
        # get rid of docs we've already seen
        del self.oplog_doc_batch[:oplog_batch_size] 
        
        #run this function every second
        threading.Timer(1, retrieve_docs).start()      
                
            #at this point, all docs are added to the queue
    
    def get_oplog_cursor(self, timestamp):
        """Move cursor to the proper place in the oplog. 
        """
        ret = None
        
        if timestamp is not None:
            cursor = self.oplog.find(spec={'op':{'$ne':'n'}, 
            'ts':{'$gte':timestamp}}, tailable=True, order={'$natural':'asc'})
          #  doc = get_next_document(cursor)     
            ret = cursor
            
        return ret
        
    #never used, will probably get rid of soon
    def get_last_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural',pymongo.DESCENDING).limit(1)
        return curr[0]['ts']
        
        
    def get_first_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural',pymongo.ASCENDING).limit(1)
        return curr[0]['ts']
        
        
    def init_cursor(self):
        """Position the cursor appropriately.
        
        The cursor is set to either the beginning of the oplog, or wherever it was 
        last left off. 
        """
        timestamp = self.read_config()
        
        if timestamp is None:
            timestamp = self.get_first_oplog_timestamp()
            
        self.checkpoint = Checkpoint()
        self.checkpoint.commit_ts = timestamp
        cursor = self.get_oplog_cursor(timestamp)
        
        return cursor
            
        
    def prepare_for_sync(self):
        """ Initializes the cursor for the sync method. 
        """
        cursor = None
        last_commit = None

        if self.checkpoint is None:
            cursor = self.init_cursor()
        else:
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
        os.rename(self.oplog_file, self.oplog_file + '~')  # temp file
        dest = open(self.oplog_file, 'w')
        source = open(self.oplog_file + '~', 'r')
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
        
        
        #need to consider how to get one global config file for all primary oplogs
    def read_config(self):
        """Read the config file for the relevant timestamp, if possible.
        """      
        config_file = self.oplog_file
        if config_file is None:
            print 'Need a config file!'
            return None
        
        source = open(self.oplog_file, 'r')
        data = json.load(source)
        oplog_str = str(self.oplog.database.connection)
        
        count = 0
        while (count < len(data)):
            if oplog_str in data[count]:  #next line has timestamp
                count = count + 1
                self.checkpoint.commit_ts = data[count]
                break
            count = count + 2 # skip current coll and timestmap
                
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    