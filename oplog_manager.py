"""Tails the oplog of a shard and returns entries"""

import os
import time
import json
import pymongo

from pymongo import Connection
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from threading import Thread, Timer
from checkpoint import Checkpoint
from solr_doc_manager import SolrDocManager
from util import (bson_ts_to_long,
                  long_to_bson_ts)


class OplogThread(Thread):
    """OplogThread gathers the updates for a single oplog. 
    """
    
    def __init__(self, primary_conn, mongos_address, oplog_coll, is_sharded,
     doc_manager, oplog_file, namespace_set):
        """Initialize the oplog thread.
        """
        super(OplogThread, self).__init__()
        self.primary_connection = primary_conn
        self.mongos_address = mongos_address
        self.oplog = oplog_coll
        self.is_sharded = is_sharded
        self.doc_manager = doc_manager
        self.running = False
        self.checkpoint = None
        self.oplog_file = oplog_file
        self.namespace_set = namespace_set 
        self.mongos_connection = Connection(mongos_address)
        
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
            

            for entry in cursor:  
                operation = entry['op']

                if operation == 'd':
                    doc_id = entry['o']['_id']
                    self.doc_manager.remove(doc_id)
                
                elif operation == 'i' or operation == 'u':
                    doc = self.retrieve_doc(entry)
                    if doc is not None:
                        doc['ts'] = bson_ts_to_long(entry['ts'])
                        doc['ns'] = entry['ns']
                        self.doc_manager.upsert([doc])
                    
                last_ts = entry['ts']
            
            if last_ts is not None:                 #we actually processed docs
                self.checkpoint.commit_ts = last_ts
                self.write_config()
                
            time.sleep(2)   #for testing purposes
            
    
    
    def stop(self):
        """Stop this thread from managing the oplog.
        """
        self.running = False
            
            
    def retrieve_doc(self, entry):
        """Given the doc ID's, retrieve those documents from the mongos.
        """
        namespace = entry['ns']
        doc_id = entry['o']['_id']

        db_name, coll_name = namespace.split('.',1)
        coll = self.mongos_connection[db_name][coll_name]
        doc = coll.find_one({'_id':doc_id})
      
        return doc
    
    
    def get_oplog_cursor(self, timestamp):
        """Move cursor to the proper place in the oplog. 
        """
        ret = None
        
        if timestamp is None:
            return None
            
        print 'timestamp is'
        print timestamp
            
        cursor = self.oplog.find({'ts': {'$gte': timestamp}}, tailable=True,
            await_data=True).sort('$natural', pymongo.ASCENDING) 
    
        try: 
            # we should re-read the last committed document
            doc = cursor.next() 
            if timestamp == doc['ts']:   
                ret = cursor 
            else:
                print 'oplog stale'
                return None
        except:
            entry = self.oplog.find_one({'ts':timestamp})
            
            if entry is None:
                print 'entry is None'
                time.sleep(2)
                less_than_doc = self.oplog.find_one({'ts': {'$lt':timestamp}})
                print 'lt doc is '
                print less_than_doc
                if less_than_doc:
                    time.sleep(1)
                    ret = self.get_oplog_cursor(self.rollback())
            else:
                ret = cursor
        
        return ret
        
    def get_last_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural',pymongo.DESCENDING).limit(1)
        return curr[0]['ts']
        
    #used here for testing, eventually we will use last_oplog_ts() + full_dump()
    def get_first_oplog_timestamp(self):
        """Return the timestamp of the first entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural',pymongo.ASCENDING).limit(1)
        return curr[0]['ts']
        
    
    def dump_collection(self):
        """Dumps collection into backend engine.
        
        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """
        for namespace in self.namespace_set:
            db, coll = namespace.split('.', 1)
            cursor = self.primary_connection[db][coll].find()

            for doc in cursor:
                self.doc_manager.upsert([doc])
            
        
    def init_cursor(self):
        """Position the cursor appropriately.
        
        The cursor is set to either the beginning of the oplog, or wherever it was 
        last left off. 
        """
        timestamp = self.read_config()
        
        if timestamp is None:
            timestamp = self.get_last_oplog_timestamp()
            self.dump_collection()
            
        self.checkpoint.commit_ts = timestamp
        self.write_config()
        cursor = self.get_oplog_cursor(timestamp)
        
        return cursor
            
        
    def prepare_for_sync(self):
        """ Initializes the cursor for the sync method. 
        """
        cursor = None
        last_commit = None

        if self.checkpoint is None:
            self.checkpoint = Checkpoint()
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
        
        timestamp = bson_ts_to_long(self.checkpoint.commit_ts)
        json_str = json.dumps([oplog_str, timestamp])
        dest.write(json_str) 
            
        for line in source:
            if oplog_str in line:
                continue                        # we've already updated
            else:
                dest.write(line)
  
        
        source.close()
        dest.close()
        os.remove(self.oplog_file+'~')
        

    def read_config(self):
        """Read the config file for the relevant timestamp, if possible.
        """      
        config_file = self.oplog_file
        if config_file is None:
            print 'Need a config file!'
            return None
        
        source = open(self.oplog_file, 'r')
        try: 
            data = json.load(source)
        except:                                             # empty file
            return None
        
        oplog_str = str(self.oplog.database.connection)
        
        count = 0
        while (count < len(data)):
            if oplog_str in data[count]:                    #next line has time
                count = count + 1
                self.checkpoint.commit_ts = long_to_bson_ts(data[count])
                break
            count = count + 2                               # skip to next set
            
        return self.checkpoint.commit_ts
        
    
    def rollback(self):
        """Rollback backend engine to consistent state. 
        
        The strategy is to find the latest timestamp in the backend and 
        the largest timestamp in the oplog less than the latest backend
        timestamp. This defines the rollback window and we just roll these
        back until the oplog and backend are in consistent states. 
        """
        last_inserted_doc = self.doc_manager.get_last_doc()
        print 'last inserted doc is '
        print last_inserted_doc
        if last_inserted_doc is None:
            return None

        backend_ts = long_to_bson_ts(last_inserted_doc['ts'])
        last_oplog_entry = self.oplog.find_one({ 'ts': { '$lt':backend_ts} }, 
        sort= [('$natural',pymongo.DESCENDING)])
        
        print 'last oplog entry is'
        print last_oplog_entry
        time.sleep(1)
        if last_oplog_entry is None:
            return None
            
        rollback_cutoff_ts = last_oplog_entry['ts']
        start_ts = bson_ts_to_long(rollback_cutoff_ts)
        end_ts = last_inserted_doc['ts']    
        
        query = 'ts: [%s TO %s]' % (start_ts, end_ts)
        print 'start_ts %s, end_ts %s' % (start_ts, end_ts)
        docs_to_rollback = self.doc_manager.search(query)   
        
        rollback_set = {}
        for doc in docs_to_rollback:
            ns = doc['ns']
            
            if rollback_set.has_key(ns):
                rollback_set[ns].append(doc['_id'])
            else:
                rollback_set[ns] = [doc['_id']]
                
        for namespace, id_list in rollback_set:
            db, coll = namespace.split('.', 1)
            bson_obj_id_list = [ObjectId(x) for x in id_list]
            
            to_update = [self.mongos_connection.db.coll.find({'_id': 
            {'$in': bson_obj_id_list}})]
            
            id_list_set = set(id_list)
            to_index = []
            for doc in to_update:
                id_list.set.remove(str(doc['_id']))
                to_index.append(doc)
            
            #delete the inconsistent documents
            for id in id_list_set:
                self.doc_manager.remove(id)
                
            for doc in to_index:
                doc['ts'] = rollback_cutoff_timestamp
                self.doc_manager.upsert(doc)
                
        return rollback_cutoff_timestamp
                       
                
            
            
            
            
            
            
            
            
            
            
            
            
        
        
        
        
        
    