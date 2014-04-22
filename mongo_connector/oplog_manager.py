# Copyright 2013-2014 MongoDB, Inc.
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

"""Tails the oplog of a shard and returns entries
"""

import bson
import logging
try:
    import Queue as queue
except ImportError:
    import queue
import pymongo
import sys
import time
import threading
import traceback
from mongo_connector import errors, util
from mongo_connector.constants import DEFAULT_BATCH_SIZE
from mongo_connector.util import retry_until_ok

from pymongo import MongoClient


class OplogThread(threading.Thread):
    """OplogThread gathers the updates for a single oplog.
    """
    def __init__(self, primary_conn, main_address, oplog_coll, is_sharded,
                 doc_manager, oplog_progress_dict, namespace_set, auth_key,
                 auth_username, repl_set=None, collection_dump=True,
                 batch_size=DEFAULT_BATCH_SIZE, fields=None,
                 dest_mapping={}):
        """Initialize the oplog thread.
        """
        super(OplogThread, self).__init__()

        self.batch_size = batch_size

        #The connection to the primary for this replicaSet.
        self.primary_connection = primary_conn

        #Boolean chooses whether to dump the entire collection if no timestamp
        # is present in the config file
        self.collection_dump = collection_dump

        #The mongos for sharded setups
        #Otherwise the same as primary_connection.
        #The value is set later on.
        self.main_connection = None

        #The connection to the oplog collection
        self.oplog = oplog_coll

        #Boolean describing whether the cluster is sharded or not
        self.is_sharded = is_sharded

        #A document manager for each target system.
        #These are the same for all threads.
        if type(doc_manager) == list:
            self.doc_managers = doc_manager
        else:
            self.doc_managers = [doc_manager]

        #Boolean describing whether or not the thread is running.
        self.running = True

        #Stores the timestamp of the last oplog entry read.
        self.checkpoint = None

        #A dictionary that stores OplogThread/timestamp pairs.
        #Represents the last checkpoint for a OplogThread.
        self.oplog_progress = oplog_progress_dict

        #The set of namespaces to process from the mongo cluster.
        self.namespace_set = namespace_set

        #The dict of source namespaces to destination namespaces
        self.dest_mapping = dest_mapping

        #If authentication is used, this is an admin password.
        self.auth_key = auth_key

        #This is the username used for authentication.
        self.auth_username = auth_username

        # List of fields to export
        self.fields = fields

        logging.info('OplogThread: Initializing oplog thread')

        if is_sharded:
            self.main_connection = MongoClient(main_address)
        else:
            self.main_connection = MongoClient(main_address,
                                               replicaSet=repl_set)
            self.oplog = self.main_connection['local']['oplog.rs']

        if auth_key is not None:
            #Authenticate for the whole system
            self.primary_connection['admin'].authenticate(
                auth_username, auth_key)
            self.main_connection['admin'].authenticate(
                auth_username, auth_key)
        if not self.oplog.find_one():
            err_msg = 'OplogThread: No oplog for thread:'
            logging.warning('%s %s' % (err_msg, self.primary_connection))

    def run(self):
        """Start the oplog worker.
        """
        logging.debug("OplogThread: Run thread started")
        while self.running is True:
            logging.debug("OplogThread: Getting cursor")
            cursor = self.init_cursor()
            logging.debug("OplogThread: Got the cursor, go go go!")

            # we've fallen too far behind
            if cursor is None and self.checkpoint is not None:
                err_msg = "OplogThread: Last entry no longer in oplog"
                effect = "cannot recover!"
                logging.error('%s %s %s' % (err_msg, effect, self.oplog))
                self.running = False
                continue

            #The only entry is the last one we processed
            if cursor is None or util.retry_until_ok(cursor.count) == 1:
                logging.debug("OplogThread: Last entry is the one we "
                              "already processed.  Up to date.  Sleeping.")
                time.sleep(1)
                continue

            last_ts = None
            err = False
            remove_inc = 0
            upsert_inc = 0
            try:
                logging.debug("OplogThread: about to process new oplog "
                              "entries")
                while cursor.alive and self.running:
                    logging.debug("OplogThread: Cursor is still"
                                  " alive and thread is still running.")
                    for n, entry in enumerate(cursor):

                        logging.debug("OplogThread: Iterating through cursor,"
                                      " document number in this cursor is %d"
                                      % n)
                        # Break out if this thread should stop
                        if not self.running:
                            break

                        # Don't replicate entries resulting from chunk moves
                        if entry.get("fromMigrate"):
                            continue

                        #sync the current oplog operation
                        operation = entry['op']
                        ns = entry['ns']

                        # use namespace mapping if one exists
                        ns = self.dest_mapping.get(entry['ns'], ns)

                        #delete
                        try:
                            logging.debug("OplogThread: Operation for this "
                                          "entry is %s" % str(operation))
                            if operation == 'd':
                                entry['_id'] = entry['o']['_id']
                                for dm in self.doc_managers:
                                    remove_inc += 1
                                    dm.remove(entry)
                            #insert/update. They are equal because of lack
                            #of support for partial update
                            elif operation == 'i' or operation == 'u':
                                doc = self.retrieve_doc(entry)
                                if doc is not None:
                                    doc['_ts'] = util.bson_ts_to_long(
                                        entry['ts'])
                                    doc['ns'] = ns
                                    for dm in self.doc_managers:
                                        upsert_inc += 1
                                        dm.upsert(doc)
                        except errors.OperationFailed:
                            logging.error(
                                "Unable to %s doc with id %s" % (
                                    "delete" if operation == "d" else "upsert",
                                    str(entry['_id'])
                                ))
                        except errors.ConnectionFailed:
                            logging.error(
                                "Network error while trying to %s %s" % (
                                    "delete" if operation == "d" else "upsert",
                                    str(entry['_id'])
                                ))

                        if (remove_inc + upsert_inc) % 1000 == 0:
                            logging.debug("OplogThread: Removed %d "
                                          " documents and upserted %d "
                                          " documents so far"
                                          % (remove_inc, upsert_inc))

                        logging.debug("OplogThread: Doc is processed.")

                        last_ts = entry['ts']

                        # update timestamp per batch size
                        # n % -1 (default for self.batch_size) == 0 for all n
                        if n % self.batch_size == 1 and last_ts is not None:
                            self.checkpoint = last_ts
                            self.update_checkpoint()

                    # update timestamp after running through oplog
                    if last_ts is not None:
                        logging.debug("OplogThread: updating checkpoint after"
                                      "processing new oplog entries")
                        self.checkpoint = last_ts
                        self.update_checkpoint()

            except (pymongo.errors.AutoReconnect,
                    pymongo.errors.OperationFailure,
                    pymongo.errors.ConfigurationError):
                err = True

            if err is True and self.auth_key is not None:
                self.primary_connection['admin'].authenticate(
                    self.auth_username, self.auth_key)
                self.main_connection['admin'].authenticate(
                    self.auth_username, self.auth_key)
                err = False

            # update timestamp before attempting to reconnect to MongoDB,
            # after being join()'ed, or if the cursor closes
            if last_ts is not None:
                logging.debug("OplogThread: updating checkpoint after an "
                              "Exception, cursor closing, or join() on this"
                              "thread.")
                self.checkpoint = last_ts
                self.update_checkpoint()

            logging.debug("OplogThread: Sleeping.  This batch I removed %d "
                          " documents and I upserted %d documents."
                          % (remove_inc, upsert_inc))
            time.sleep(2)

    def join(self):
        """Stop this thread from managing the oplog.
        """
        logging.debug("OplogThread: exiting due to join call.")
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

        doc = util.retry_until_ok(
            coll.find_one,
            {'_id': doc_id},
            fields=self.fields
        )

        return doc

    def get_oplog_cursor(self, timestamp):
        """Move cursor to the proper place in the oplog.
        """

        logging.debug("OplogThread: Getting the oplog cursor and moving it "
                      "to the proper place in the oplog.")

        if timestamp is None:
            return None

        cursor, cursor_len = None, 0
        while (True):
            try:
                logging.debug("OplogThread: Getting the oplog cursor "
                              "in the while true loop for get_oplog_cursor")
                if not self.namespace_set:
                    cursor = self.oplog.find(
                        {'ts': {'$gte': timestamp}},
                        tailable=True, await_data=True
                    )
                else:
                    cursor = self.oplog.find(
                        {'ts': {'$gte': timestamp},
                         'ns': {'$in': self.namespace_set}},
                        tailable=True, await_data=True
                    )
                # Applying 8 as the mask to the cursor enables OplogReplay
                cursor.add_option(8)
                logging.debug("OplogThread: Cursor created, getting a count.")
                cursor_len = cursor.count()
                logging.debug("OplogThread: Count is %d" % cursor_len)
                break
            except (pymongo.errors.AutoReconnect,
                    pymongo.errors.OperationFailure,
                    pymongo.errors.ConfigurationError):
                pass
        if cursor_len == 0:
            logging.debug("OplogThread: Initiating rollback from "
                          "get_oplog_cursor")
            #rollback, we are past the last element in the oplog
            timestamp = self.rollback()

            logging.info('Finished rollback')
            return self.get_oplog_cursor(timestamp)
        first_oplog_entry = retry_until_ok(lambda: cursor[0])
        cursor_ts_long = util.bson_ts_to_long(first_oplog_entry.get("ts"))
        given_ts_long = util.bson_ts_to_long(timestamp)
        if cursor_ts_long > given_ts_long:
            # first entry in oplog is beyond timestamp, we've fallen behind!
            return None
        elif cursor_len == 1:     # means we are the end of the oplog
            self.checkpoint = timestamp
            #to commit new TS after rollbacks

            return cursor
        elif cursor_len > 1:
            doc = retry_until_ok(next, cursor)
            if timestamp == doc['ts']:
                return cursor
            else:               # error condition
                logging.error('OplogThread: %s Bad timestamp in config file'
                              % self.oplog)
                return None

    def dump_collection(self):
        """Dumps collection into the target system.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """

        dump_set = self.namespace_set or []
        logging.debug("OplogThread: Dumping set of collections %s " % dump_set)

        #no namespaces specified
        if not self.namespace_set:
            db_list = retry_until_ok(self.main_connection.database_names)
            for database in db_list:
                if database == "config" or database == "local":
                    continue
                coll_list = retry_until_ok(
                    self.main_connection[database].collection_names)
                for coll in coll_list:
                    if coll.startswith("system"):
                        continue
                    namespace = str(database) + "." + str(coll)
                    dump_set.append(namespace)

        timestamp = util.retry_until_ok(self.get_last_oplog_timestamp)
        if timestamp is None:
            return None
        long_ts = util.bson_ts_to_long(timestamp)

        def docs_to_dump():
            for namespace in dump_set:
                logging.info("OplogThread: dumping collection %s"
                             % namespace)
                database, coll = namespace.split('.', 1)
                last_id = None
                attempts = 0

                # Loop to handle possible AutoReconnect
                while attempts < 60:
                    target_coll = self.main_connection[database][coll]
                    if not last_id:
                        cursor = util.retry_until_ok(
                            target_coll.find,
                            fields=self.fields,
                            sort=[("_id", pymongo.ASCENDING)]
                        )
                    else:
                        cursor = util.retry_until_ok(
                            target_coll.find,
                            {"_id": {"$gt": last_id}},
                            fields=self.fields,
                            sort=[("_id", pymongo.ASCENDING)]
                        )
                    try:
                        for doc in cursor:
                            if not self.running:
                                raise StopIteration
                            doc["ns"] = self.dest_mapping.get(
                                namespace, namespace)
                            doc["_ts"] = long_ts
                            last_id = doc["_id"]
                            yield doc
                        break
                    except pymongo.errors.AutoReconnect:
                        attempts += 1
                        time.sleep(1)

        # Extra threads (if any) that assist with collection dumps
        dumping_threads = []
        # Did the dump succeed for all target systems?
        dump_success = True
        # Holds any exceptions we can't recover from
        errors = queue.Queue()
        try:
            for dm in self.doc_managers:
                # Bulk upsert if possible
                if hasattr(dm, "bulk_upsert"):
                    logging.debug("OplogThread: Using bulk upsert function for"
                                  "collection dump")
                    # Slight performance gain breaking dump into separate
                    # threads, only if > 1 replication target
                    if len(self.doc_managers) == 1:
                        dm.bulk_upsert(docs_to_dump())
                    else:
                        def do_dump(error_queue):
                            all_docs = docs_to_dump()
                            try:
                                dm.bulk_upsert(all_docs)
                            except Exception:
                                # Likely exceptions:
                                # pymongo.errors.OperationFailure,
                                # mongo_connector.errors.ConnectionFailed
                                # mongo_connector.errors.OperationFailed
                                error_queue.put(sys.exc_info())

                        t = threading.Thread(target=do_dump, args=(errors,))
                        dumping_threads.append(t)
                        t.start()
                else:
                    logging.debug("OplogThread: DocManager %s has not"
                                  "bulk_upsert method.  Upserting documents "
                                  "serially for collection dump." % str(dm))
                    num = 0
                    for num, doc in enumerate(docs_to_dump()):
                        if num % 10000 == 0:
                            logging.debug("Upserted %d docs." % num)
                        dm.upsert(doc)
                    logging.debug("Upserted %d docs" % num)

            # cleanup
            for t in dumping_threads:
                t.join()

        except Exception:
            # See "likely exceptions" comment above
            errors.put(sys.exc_info())

        # Print caught exceptions
        try:
            while True:
                klass, value, trace = errors.get_nowait()
                dump_success = False
                traceback.print_exception(klass, value, trace)
        except queue.Empty:
            pass

        if not dump_success:
            err_msg = "OplogThread: Failed during dump collection"
            effect = "cannot recover!"
            logging.error('%s %s %s' % (err_msg, effect, self.oplog))
            self.running = False
            return None

        return timestamp

    def get_last_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        if not self.namespace_set:
            curr = self.oplog.find().sort(
                '$natural', pymongo.DESCENDING
            ).limit(1)
        else:
            curr = self.oplog.find(
                {'ns': {'$in': self.namespace_set}}
            ).sort('$natural', pymongo.DESCENDING).limit(1)

        if curr.count(with_limit_and_skip=True) == 0:
            return None

        logging.debug("OplogThread: Last oplog entry has timestamp %d."
                      % curr[0]['ts'].time)
        return curr[0]['ts']

    def init_cursor(self):
        """Position the cursor appropriately.

        The cursor is set to either the beginning of the oplog, or
        wherever it was last left off.
        """
        logging.debug("OplogThread: Initializing the oplog cursor.")
        timestamp = self.read_last_checkpoint()

        if timestamp is None and self.collection_dump:
            timestamp = self.dump_collection()
            if timestamp:
                msg = "Dumped collection into target system"
                logging.info('OplogThread: %s %s'
                             % (self.oplog, msg))
        elif timestamp is None:
            # set timestamp to top of oplog
            timestamp = retry_until_ok(self.get_last_oplog_timestamp)

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
            logging.debug("OplogThread: oplog checkpoint updated to %s" %
                          str(self.checkpoint))

    def read_last_checkpoint(self):
        """Read the last checkpoint from the oplog progress dictionary.
        """
        oplog_str = str(self.oplog)
        ret_val = None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            if oplog_str in oplog_dict.keys():
                ret_val = oplog_dict[oplog_str]

        logging.debug("OplogThread: reading last checkpoint as %s " %
                      str(ret_val))
        return ret_val

    def rollback(self):
        """Rollback target system to consistent state.

        The strategy is to find the latest timestamp in the target system and
        the largest timestamp in the oplog less than the latest target system
        timestamp. This defines the rollback window and we just roll these
        back until the oplog and target system are in consistent states.
        """
        # Find the most recently inserted document in each target system
        logging.debug("OplogThread: Initiating rollback sequence to bring "
                      "system into a consistent state.")
        last_docs = []
        for dm in self.doc_managers:
            dm.commit()
            last_docs.append(dm.get_last_doc())

        # Of these documents, which is the most recent?
        last_inserted_doc = max(last_docs,
                                key=lambda x: x["_ts"] if x else float("-inf"))

        # Nothing has been replicated. No need to rollback target systems
        if last_inserted_doc is None:
            return None

        # Find the oplog entry that touched the most recent document.
        # We'll use this to figure where to pick up the oplog later.
        target_ts = util.long_to_bson_ts(last_inserted_doc['_ts'])
        last_oplog_entry = util.retry_until_ok(
            self.oplog.find_one,
            {'ts': {'$lte': target_ts}},
            sort=[('$natural', pymongo.DESCENDING)]
        )

        logging.debug("OplogThread: last oplog entry is %s"
                      % str(last_oplog_entry))

        # The oplog entry for the most recent document doesn't exist anymore.
        # If we've fallen behind in the oplog, this will be caught later
        if last_oplog_entry is None:
            return None

        # rollback_cutoff_ts happened *before* the rollback
        rollback_cutoff_ts = last_oplog_entry['ts']
        start_ts = util.bson_ts_to_long(rollback_cutoff_ts)
        # timestamp of the most recent document on any target system
        end_ts = last_inserted_doc['_ts']

        for dm in self.doc_managers:
            rollback_set = {}   # this is a dictionary of ns:list of docs

            # group potentially conflicted documents by namespace
            for doc in dm.search(start_ts, end_ts):
                if doc['ns'] in rollback_set:
                    rollback_set[doc['ns']].append(doc)
                else:
                    rollback_set[doc['ns']] = [doc]

            # retrieve these documents from MongoDB, either updating
            # or removing them in each target system
            for namespace, doc_list in rollback_set.items():
                # Get the original namespace
                original_namespace = namespace
                for source_name, dest_name in self.dest_mapping.items():
                    if dest_name == namespace:
                        original_namespace = source_name

                database, coll = original_namespace.split('.', 1)
                obj_id = bson.objectid.ObjectId
                bson_obj_id_list = [obj_id(doc['_id']) for doc in doc_list]

                to_update = util.retry_until_ok(
                    self.main_connection[database][coll].find,
                    {'_id': {'$in': bson_obj_id_list}},
                    fields=self.fields
                )
                #doc list are docs in target system, to_update are
                #docs in mongo
                doc_hash = {}  # hash by _id
                for doc in doc_list:
                    doc_hash[bson.objectid.ObjectId(doc['_id'])] = doc

                to_index = []

                def collect_existing_docs():
                    for doc in to_update:
                        if doc['_id'] in doc_hash:
                            del doc_hash[doc['_id']]
                            to_index.append(doc)
                retry_until_ok(collect_existing_docs)

                #delete the inconsistent documents
                logging.debug("OplogThread: Rollback, removing inconsistent "
                              "docs.")
                remov_inc = 0
                for doc in doc_hash.values():
                    try:
                        dm.remove(doc)
                        remov_inc += 1
                        logging.debug("OplogThread: Rollback, removed %s " %
                                      str(doc))
                    except errors.OperationFailed:
                        logging.warning(
                            "Could not delete document during rollback: %s "
                            "This can happen if this document was already "
                            "removed by another rollback happening at the "
                            "same time." % str(doc)
                        )

                logging.debug("OplogThread: Rollback, removed %d docs." %
                              remov_inc)

                #insert the ones from mongo
                logging.debug("OplogThread: Rollback, inserting documents "
                              "from mongo.")
                insert_inc = 0
                fail_insert_inc = 0
                for doc in to_index:
                    doc['_ts'] = util.bson_ts_to_long(rollback_cutoff_ts)
                    doc['ns'] = self.dest_mapping.get(namespace, namespace)
                    try:
                        insert_inc += 1
                        dm.upsert(doc)
                    except errors.OperationFailed as e:
                        fail_insert_inc += 1
                        logging.error("OplogThread: Rollback, Unable to "
                                      "insert %s with exception %s"
                                      % (doc, str(e)))

        logging.debug("OplogThread: Rollback, Successfully inserted %d "
                      " documents and failed to insert %d"
                      " documents.  Returning a rollback cutoff time of %s "
                      % (insert_inc, fail_insert_inc, str(rollback_cutoff_ts)))

        return rollback_cutoff_ts
