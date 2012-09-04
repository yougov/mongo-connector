# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Tails the oplog of a shard and returns entries
"""

import bson
import inspect
import json
import logging
import os
import pymongo
import sys
import time
import threading
import util


class OplogThread(threading.Thread):
    """OplogThread gathers the updates for a single oplog.
    """
    def __init__(self, primary_conn, main_address, oplog_coll, is_sharded,
                 doc_manager, oplog_progress_dict, namespace_set, auth_key,
                 auth_username, repl_set=None):
        """Initialize the oplog thread.
        """
        super(OplogThread, self).__init__()

        #The connection to the primary for this replicaSet.
        self.primary_connection = primary_conn

        #The mongos for sharded setups
        #Otherwise the same as primary_connection.
        #The value is set later on.
        self.main_connection = None

        #The connection to the oplog collection
        self.oplog = oplog_coll

        #Boolean describing whether the cluster is sharded or not
        self.is_sharded = is_sharded

        #The document manager for the target system.
        #This is the same for all threads.
        self.doc_manager = doc_manager

        #Boolean describing whether or not the thread is running.
        self.running = True

        #Stores the timestamp of the last oplog entry read.
        self.checkpoint = None

        #A dictionary that stores OplogThread/timestamp pairs.
        #Represents the last checkpoint for a OplogThread.
        self.oplog_progress = oplog_progress_dict

        #The set of namespaces to process from the mongo cluster.
        self.namespace_set = namespace_set

        #If authentication is used, this is an admin password.
        self.auth_key = auth_key

        #This is the username used for authentication.
        self.auth_username = auth_username

        logging.info('OplogManager: Initializing oplog thread')

        if is_sharded:
            self.main_connection = pymongo.Connection(main_address)
        else:
            self.main_connection = pymongo.Connection(main_address,
                                                      replicaSet=repl_set)
            self.oplog = self.main_connection['local']['oplog.rs']

        if auth_key is not None:
            #Authenticate for the whole system
            primary_conn['admin'].authenticate(auth_username, auth_key)
        if self.oplog.find().count() == 0:
            err_msg = 'OplogThread: No oplog for thread:'
            logging.error('%s %s' % (err_msg, self.primary_connection))
            self.running = False

    def run(self):
        """Start the oplog worker.
        """
        while self.running is True:
            cursor = self.init_cursor()

            # we've fallen too far behind
            if cursor is None and self.checkpoint is not None:
                err_msg = "OplogManager: Last entry no longer in oplog"
                effect = "cannot recover!"
                logging.error('%s %s %s' % (err_msg, effect, self.oplog))
                self.running = False
                continue

            #The only entry is the last one we processed
            if util.retry_until_ok(cursor.count) == 1:
                time.sleep(1)
                continue

            last_ts = None
            err = False
            try:
                for entry in cursor:
                    #sync the current oplog operation
                    operation = entry['op']
                    ns = entry['ns']

                    #check if ns is excluded or not.
                    #also ensure non-empty namespace set.
                    if ns not in self.namespace_set and self.namespace_set:
                        continue

                    #delete
                    if operation == 'd':
                        entry['_id'] = entry['o']['_id']
                        self.doc_manager.remove(entry)
                    #insert/update. They are equal because of lack of support
                    #for partial update
                    elif operation == 'i' or operation == 'u':
                        doc = self.retrieve_doc(entry)
                        if doc is not None:
                            doc['_ts'] = util.bson_ts_to_long(entry['ts'])
                            doc['ns'] = ns
                            self.doc_manager.upsert(doc)

                    last_ts = entry['ts']
            except (pymongo.errors.AutoReconnect,
                    pymongo.errors.OperationFailure):
                err = True
                pass

            if err is True and self.auth_key is not None:
                primary_conn['admin'].authenticate(self.auth_username,
                                                   self.auth_key)
                err = False

            if last_ts is not None:
                self.checkpoint = last_ts
                self.update_checkpoint()

            time.sleep(2)

    def join(self):
        """Stop this thread from managing the oplog.
        """
        self.running = False
        threading.Thread.join(self)

    def retrieve_doc(self, entry):
        """Given the doc ID's, retrieve those documents from the mongos.
        """

        if not entry:
            return None

        namespace = entry['ns']

        # Update operations don't have an 'o' field specifying the document
        #- instead it specifies
        # the changes. So we use 'o2' for updates to get the doc_id later.

        if 'o2' in entry:
            doc_field = 'o2'
        else:
            doc_field = 'o'

        doc_id = entry[doc_field]['_id']
        db_name, coll_name = namespace.split('.', 1)

        coll = self.main_connection[db_name][coll_name]
        doc = util.retry_until_ok(coll.find_one, {'_id': doc_id})

        return doc

    def get_oplog_cursor(self, timestamp):
        """Move cursor to the proper place in the oplog.
        """

        if timestamp is None:
            return None
        cursor = util.retry_until_ok(self.oplog.find,
                                     {'ts': {'$lte': timestamp}})

        if (util.retry_until_ok(cursor.count)) == 0:
            return None

        # Check to see if cursor is too stale

        while (True):
            try:
                cursor = self.oplog.find({'ts': {'$gte': timestamp}},
                                         tailable=True, await_data=True)
                cursor = cursor.sort('$natural', pymongo.ASCENDING)
                cursor_len = cursor.count()
                break
            except (pymongo.errors.AutoReconnect,
                    pymongo.errors.OperationFailure):
                pass
        if cursor_len == 1:     # means we are the end of the oplog
            self.checkpoint = timestamp
            #to commit new TS after rollbacks

            return cursor
        elif cursor_len > 1:
            doc = next(cursor)
            if timestamp == doc['ts']:
                return cursor
            else:               # error condition
                logging.error('%s Bad timestamp in config file' % self.oplog)
                return None
        else:
            #rollback, we are past the last element in the oplog
            timestamp = self.rollback()

            logging.info('Finished rollback')
            return self.get_oplog_cursor(timestamp)

    def dump_collection(self):
        """Dumps collection into the target system.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """

        dump_set = self.namespace_set

        #no namespaces specified
        if not self.namespace_set:
            db_list = self.main_connection.database_names()
            for db in db_list:
                if db == "config" or db == "local":
                    continue
                coll_list = self.main_connection[db].collection_names()
                for coll in coll_list:
                    if coll.startswith("system"):
                        continue
                    namespace = str(db) + "." + str(coll)
                    dump_set.append(namespace)

        timestamp = util.retry_until_ok(self.get_last_oplog_timestamp)
        if timestamp is None:
            return None
        for namespace in dump_set:
            db, coll = namespace.split('.', 1)
            target_coll = self.main_connection[db][coll]
            cursor = util.retry_until_ok(target_coll.find)
            long_ts = util.bson_ts_to_long(timestamp)

            try:
                for doc in cursor:
                    doc['ns'] = namespace
                    doc['_ts'] = long_ts
                    self.doc_manager.upsert(doc)
            except (pymongo.errors.AutoReconnect,
                    pymongo.errors.OperationFailure):

                err_msg = "OplogManager: Failed during dump collection"
                effect = "cannot recover!"
                logging.error('%s %s %s' % (err_msg, effect, self.oplog))
                self.running = False
                return

        return timestamp

    def get_last_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        curr = self.oplog.find().sort('$natural', pymongo.DESCENDING).limit(1)
        if curr.count(with_limit_and_skip=True) == 0:
            return None

        return curr[0]['ts']

    def init_cursor(self):
        """Position the cursor appropriately.

        The cursor is set to either the beginning of the oplog, or
        wherever it was last left off.
        """
        timestamp = self.read_last_checkpoint()

        if timestamp is None:
            timestamp = self.dump_collection()
            msg = "Dumped collection into target system"
            logging.info('OplogManager: %s %s'
                         % (self.oplog, msg))

        self.checkpoint = timestamp
        cursor = self.get_oplog_cursor(timestamp)
        if cursor is not None:
            self.update_checkpoint()

        return cursor

    def update_checkpoint(self):
        """Store the current checkpoint in the oplog progress dictionary.
        """
        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            oplog_dict[str(self.oplog)] = self.checkpoint

    def read_last_checkpoint(self):
        """Read the last checkpoint from the oplog progress dictionary.
        """
        oplog_str = str(self.oplog)
        ret_val = None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            if oplog_str in oplog_dict.keys():
                ret_val = oplog_dict[oplog_str]

        return ret_val

    def rollback(self):
        """Rollback target system to consistent state.

        The strategy is to find the latest timestamp in the target system and
        the largest timestamp in the oplog less than the latest target system
        timestamp. This defines the rollback window and we just roll these
        back until the oplog and target system are in consistent states.
        """
        self.doc_manager.commit()
        last_inserted_doc = self.doc_manager.get_last_doc()

        if last_inserted_doc is None:
            return None

        target_ts = util.long_to_bson_ts(last_inserted_doc['_ts'])
        last_oplog_entry = self.oplog.find_one({'ts': {'$lte': target_ts}},
                                               sort=[('$natural',
                                               pymongo.DESCENDING)])
        if last_oplog_entry is None:
            return None

        rollback_cutoff_ts = last_oplog_entry['ts']
        start_ts = util.bson_ts_to_long(rollback_cutoff_ts)
        end_ts = last_inserted_doc['_ts']

        docs_to_rollback = self.doc_manager.search(start_ts, end_ts)

        rollback_set = {}   # this is a dictionary of ns:list of docs
        for doc in docs_to_rollback:
            ns = doc['ns']
            if ns in rollback_set:
                rollback_set[ns].append(doc)
            else:
                rollback_set[ns] = [doc]

        for namespace, doc_list in rollback_set.items():
            db, coll = namespace.split('.', 1)
            ObjId = bson.objectid.ObjectId
            bson_obj_id_list = [ObjId(doc['_id']) for doc in doc_list]

            retry = util.retry_until_ok
            to_update = retry(self.main_connection[db][coll].find,
                              {'_id': {'$in': bson_obj_id_list}})
            #doc list are docs in  target system, to_update are docs in mongo
            doc_hash = {}  # hash by _id
            for doc in doc_list:
                doc_hash[bson.objectid.ObjectId(doc['_id'])] = doc

            to_index = []
            count = 0
            while True:
                try:
                    for doc in to_update:
                        if doc['_id'] in doc_hash:
                            del doc_hash[doc['_id']]
                            to_index.append(doc)
                    break
                except (pymongo.errors.OperationFailure,
                        pymongo.errors.AutoReconnect):
                    count += 1
                    if count > 60:
                        sys.exit(1)
                    time.sleep(1)

            #delete the inconsistent documents
            for doc in doc_hash.values():
                self.doc_manager.remove(doc)

            #insert the ones from mongo
            for doc in to_index:
                doc['_ts'] = util.bson_ts_to_long(rollback_cutoff_ts)
                doc['ns'] = namespace
                self.doc_manager.upsert(doc)

        return rollback_cutoff_ts
