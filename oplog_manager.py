"""Tails the oplog of a shard and returns entries
"""

import os
import time
import fcntl
import json
import logging
import pymongo

from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from checkpoint import Checkpoint
from pymongo import Connection
from pymongo.errors import AutoReconnect
from threading import Thread, Timer
from util import (bson_ts_to_long,
                  long_to_bson_ts,
                  retry_until_ok)


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

        while self.running is True:

            cursor = self.prepare_for_sync()

            if cursor is None or retry_until_ok(cursor.count) == 1:
                time.sleep(1)
                continue

            last_ts = None
            cursor_size = retry_until_ok(cursor.count)
            counter = 0

            try:
                for entry in cursor:
                    operation = entry['op']

                    if operation == 'd':
                        entry['_id'] = entry['o']['_id']
                        self.doc_manager.remove(entry)

                    elif operation == 'i' or operation == 'u':
                        doc = self.retrieve_doc(entry)

                        if doc is not None:
                            doc['_ts'] = bson_ts_to_long(entry['ts'])
                            doc['ns'] = entry['ns']
                            self.doc_manager.upsert(doc)

                    last_ts = entry['ts']
            except AutoReconnect:
                pass

            if last_ts is not None:
                self.checkpoint.commit_ts = last_ts
                self.write_config()

            time.sleep(2)

    def stop(self):
        """Stop this thread from managing the oplog.
        """
        self.running = False

    def retrieve_doc(self, entry):
        """Given the doc ID's, retrieve those documents from the mongos.
        """
        if not entry:
            return None

        namespace = entry['ns']
        if 'o2' in entry:
            doc_field = 'o2'
        else:
            doc_field = 'o'

        doc_id = entry[doc_field]['_id']
        db_name, coll_name = namespace.split('.', 1)

        while True:
            try:
                coll = self.mongos_connection[db_name][coll_name]
                doc = coll.find_one({'_id': doc_id})
                break
            except AutoReconnect:
                time.sleep(1)
                continue

        return doc

    def get_oplog_cursor(self, timestamp):
        """Move cursor to the proper place in the oplog.
        """

        if timestamp is None:
            return None

        while (True):
            try:
                cursor = self.oplog.find({'ts': {'$gte': timestamp}},
                                         tailable=True, await_data=True)
                cursor = cursor.sort('$natural', pymongo.ASCENDING)
                cursor_len = cursor.count()
                break
            except:
                pass

        if cursor_len == 1:     # means we are the end of the oplog
            return cursor
        elif cursor_len > 1:
            doc = cursor.next()
            if timestamp == doc['ts']:
                return cursor
            else:               # error condition
                logging.error('Bad timestamp in config file')
                return None
        else:
            #rollback, we are past the last element in the oplog
            timestamp = self.rollback()
            logging.info('Finished rollback')
            return self.get_oplog_cursor(timestamp)

    def get_last_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural', pymongo.DESCENDING).limit(1)
        if curr.count(with_limit_and_skip=True) == 0:
            return None

        return curr[0]['ts']

    #used here for testing
    def get_first_oplog_timestamp(self):
        """Return the timestamp of the first entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural', pymongo.ASCENDING).limit(1)
        return curr[0]['ts']

    def dump_collection(self, timestamp):
        """Dumps collection into backend engine.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """
        if timestamp is None:
            return None

        for namespace in self.namespace_set:
            db, coll = namespace.split('.', 1)
            target_coll = retry_until_ok(self.mongos_connection[db][coll],
                                         no_func=True)
            cursor = retry_until_ok(target_coll.find)
            long_ts = bson_ts_to_long(timestamp)

            for doc in cursor:
                doc['ns'] = namespace
                doc['_ts'] = long_ts
                self.doc_manager.upsert(doc)

    def init_cursor(self):
        """Position the cursor appropriately.

        The cursor is set to either the beginning of the oplog, or
        wherever it was last left off.
        """
        timestamp = self.read_config()

        if timestamp is None:
            timestamp = retry_until_ok(self.get_last_oplog_timestamp)
            self.dump_collection(timestamp)
            logging.info('OplogManager: Dumped collection into backend')

        self.checkpoint.commit_ts = timestamp
        if timestamp is not None:
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
        if self.oplog_file is None:
            return None
            
        ofile = file(self.oplog_file, 'w')
        fcntl.lockf(ofile.fileno(), fcntl.LOCK_EX)

        try:
            os.rename(self.oplog_file, self.oplog_file + '~')  # temp file
            dest = open(self.oplog_file, 'w')
            source = open(self.oplog_file + '~', 'r')
        except:
            logging.error('OplogManager: Could not open config file!')
            return None

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
        os.remove(self.oplog_file + '~')
        
        fcntl.lockf(ofile.fileno(), fcntl.LOCK_UN)
        

    def read_config(self):
        """Read the config file for the relevant timestamp, if possible.
        """
        config_file = self.oplog_file
        if config_file is None:
            return None

        source = open(self.oplog_file, 'r')
        try:
            data = json.load(source)
        except:                                             # empty file
            return None

        oplog_str = str(self.oplog.database.connection)

        count = 0
        while (count < len(data)):
            if oplog_str in data[count]:             # next line has time
                count = count + 1
                self.checkpoint.commit_ts = long_to_bson_ts(data[count])
                break
            count = count + 2                        # skip to next set

        return self.checkpoint.commit_ts

    def rollback(self):
        """Rollback backend engine to consistent state.

        The strategy is to find the latest timestamp in the backend and
        the largest timestamp in the oplog less than the latest backend
        timestamp. This defines the rollback window and we just roll these
        back until the oplog and backend are in consistent states.
        """
        self.doc_manager.commit()
        last_inserted_doc = self.doc_manager.get_last_doc()

        if last_inserted_doc is None:
            return None

        backend_ts = long_to_bson_ts(last_inserted_doc['_ts'])
        last_oplog_entry = self.oplog.find_one({'ts': {'$lt': backend_ts}},
                                               sort=[('$natural',
                                               pymongo.DESCENDING)])

        if last_oplog_entry is None:
            return None

        rollback_cutoff_ts = last_oplog_entry['ts']
        start_ts = bson_ts_to_long(rollback_cutoff_ts)
        end_ts = last_inserted_doc['_ts']

        docs_to_rollback = self.doc_manager.search(start_ts, end_ts)

        rollback_set = {}
        for doc in docs_to_rollback:
            ns = doc['ns']
            if ns in rollback_set:
                rollback_set[ns].append(doc)
            else:
                rollback_set[ns] = [doc]

        for namespace, doc_list in rollback_set.iteritems():
            db, coll = namespace.split('.', 1)
            bson_obj_id_list = [ObjectId(doc['_id']) for doc in doc_list]

            to_update = retry_until_ok(self.mongos_connection[db][coll].find,
                                       {'_id': {'$in': bson_obj_id_list}})

            doc_hash = {}
            for doc in doc_list:
                doc_hash[ObjectId(doc['_id'])] = doc

            to_index = []

            for doc in to_update:
                if doc['_id'] in doc_hash:
                    del doc_hash[doc['_id']]
                    to_index.append(doc)

            #delete the inconsistent documents
            for doc in doc_hash.values():
                self.doc_manager.remove(doc)

            for doc in to_index:
                doc['_ts'] = bson_ts_to_long(rollback_cutoff_ts)
                doc['ns'] = namespace
                self.doc_manager.upsert(doc)

        return rollback_cutoff_ts
