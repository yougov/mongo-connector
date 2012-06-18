import MySQLdb
#OPLOG_BATCH_SIZE = 200
counter = 0;

def class SphinxSynchronizer():
    
    def __init__(self, sphinx, mongo_connection, oplog, config_writer, ns_set, is_sharded):
        """
        Init the object that will synchronize with the Sphinx server to transfer documents. 
        """
        
        self.sphinx = sphinx
        self.mongo_connection = mongo_connection
        self.oplog = oplog
        self.oplog_backlog = {}
        self.config_writer = config_writer
        self.checkpoint = None
        self.ns_set = ns_set
        self.is_sharded = is_sharded
        self.stop = None
        
    def stop(self):
        """
        Stop synchronizing - won't interrupt a sync in progress. 
        """
        self.stop = True
        
    def sync(self):
        """
        Sync's the Mongo cluster with the Sphinx server.
        """
        last_timestamp = (self.checkpoint is None) ? None : self.checkpoint.commit_ts
        #get records in oplog
        cursor = init_sync()
        
        while True:
            doc_batch = []
            ns_snapshot = self.ns_set
            doc_count = 0
            
            for doc in cursor:
                if doc is None:
                    break
                if insert_to_backlog(doc):
                    continue
                
                doc_batch.append(doc)
                doc_count += 1
            
            last_timestamp = doc['ts']
            
            if doc_batch is not None:
                update_sphinx(doc_batch, true)
            #update sphinx (doc_batch)
            sleep 10  #some update interval
            
            #handle cursor failure if any
                
                
    def insert_to_backlog(self, oplog_entry):
        """
        Add an oplog entry to the backlog of docs to enter.
        """
        
        ns = oplog_entry['ns']
        inserted = false
        
        if self.oplog_backlog.has_key(ns):
            self.oplog_backlog['ns'].append(oplog_entry)
            inserted = true
        
        return inserted
        
    
    def init_sync(self, checkpoint):
        """ 
        Initializes the cursor for the sync method. 
        """
        
        cursor = None
        last_commit = None
        
        if self.checkpoint is None:
            cursor = full_dump()
        else:
            restore_checkpoint(self.checkpoint)
            last_commit = self.checkpoint.commit_ts
            
            cursor = get_oplog_cursor(last_commit)
            if cursor is None:
                cursor = full_dump()
                
        return cursor
    
    
    def get_oplog_cursor(self, timestamp):
        """
        Returns the cursor to the place in the oplog just after the last recorded 
        timestamp 
        """
        ret = None
        
        if timestamp is not None:
            cursor = self.oplog_collection.find(spec={'op':{'$ne':'n'}, 'ts':{'$gte':0}}, 
            tailable=True, order={'$natural':'asc'})
            doc = cursor.next_document     
            
        #means we didn't get anything from the cursor, or no timestamp 
        if doc is None or cursor.__getitem__() is None:
            entry = self.oplog_collection.find_one(spec={'ts':timestamp})
            
            if entry is None:
                sub_doc = self.oplog_collection.find_one(spec={'ts':{'$lt':timestamp}})
                
            if sub_doc is None:
                #uh oh - rollback
            
            ret = cursor
            
        elif timestamp == doc['ts']
            ret = cursor
        
        return ret
        
    def get_last_oplog_timestamp(self):
        """
        Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog_collection.find(sort={'$natural':'desc'},limit=1)
        return curr.__getitem__('ts')
        
        
    def full_dump(self):
        """
        Dumps the database, returns the cursor to the latest entry just before the dump
        is performed.
        """
        timestamp = get_last_oplog_timestamp()
        cursor = get_oplog_cursor(timestamp)
        
        for ns,field in self.ns_set:
            dump_collection(ns,timestamp)
        
        return cursor
    
    def dump_collection(self, namespace, timestamp):
        """
        Dump the contents of the namespace to Sphinx.
        """
        db_name, collection = get_ns_details(namespace)
        cursor = self.oplog_collection.db.connection[db_name][collection].find()
        # if unsharded, use self.db_connection[db_name]....
        
        #write to update total dump count
        #write to reset dump count for namespace
        
        for doc in cursor:
            #sphinx.add(prepare_doc(doc, namespace, timestamp))
            #increment dump count for writer
            
        #sphinx commit
        #write update timestamp, update commit
        
            
    def update_sphinx(self, oplog_doc_entries, do_timestamp_commit = None):
        """
        Update all the relevant documents in the Sphinx server. 
        """
        update_list = {}
        timestamp = None
        
        for entry in oplog_doc_entries:
            namespace = entry['ns']
            doc = entry['o']
            timestamp = entry['ts']
            operation = entry['op']
            
            if operation == 'i':  #insert
                if is_sharded:
                    id = entry['o']['_id']
                    db_name, coll_name = get_namespace_details(namespace)
                    
                    cursor = self.mongo_connection[db_name][coll_name].find({'_id':id})
                    count = cursor.count
                    if count != 0:          # it's in mongos => part of a migration
                        continue
                
                #TODO prepare and add sphinx doc
                #TODO update configuration writer
                
            elif operation == 'u': #update
                update_list[namespace] = update_list[namespace] or []
                to_update = update_list[namespace]
                to_update.append(oplog_entry['o2']['_id']
                
            elif operation == 'd': #delete
                update_list[namespace] = update_list[namespace] or []
                to_update = update_list[namespace]
                id = oplog_entry['o']['_id']
                
                if is_sharded:
                    db_name, coll_name = get_namespace_details(namespace)
                    cursor = self.mongo_connection[db_name][coll_name].find({'_id':id})
                    count = cursor.count
                    
                    if count != 0:
                        continue
                
                to_update.remove(id)
                #TODO prepare and add doc to sphinx
                
            elif operation == 'n':
                #do nothing here
            
            for namespace, id_list in update_list:
                database, collection = get_namespace_details(namespace)
                
                to_update = self.mongo_connection.db(database).collection(collection).
                find({'_id':{'in':id_list}})
                
                for doc in to_update:
                    #TODO prepare and add doc to sphinx
                    
                id_list.remove doc['_id']
        
        #TODO commit to sphinx     
        
        
    def prepare_sphinx_doc(doc, ns, ts):
        """
        Extract the relevant fields to insert into Sphinx. 
        """
        #all keys are stored as strings, so no issues
        keys = str(",".join(dict.keys()))
        keys = "(id, " + keys + ")"
    
        #need to lock counter
        values = "( " + counter + " "
        for value in dict.values():
            values += "\'" + str(value) + "\',"
        
        #corresponding values for insert query, delete trailing comma
        values = values[:-1] + ")"
        
        return keys, values
        
            
                  
              
                
            
        
        
    
    
        
        
        
        
        
        
        
        
        
        
        
        
        
            