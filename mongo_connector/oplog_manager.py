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

import logging
try:
    import Queue as queue
except ImportError:
    import queue
import pymongo
import sys
import time
import threading

from mongo_connector import errors, util
from mongo_connector.barrier import Barrier
from mongo_connector.constants import DEFAULT_BATCH_SIZE
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector.util import log_fatal_exceptions, retry_until_ok
from bson.objectid import ObjectId
from elasticsearch.exceptions import NotFoundError

LOG = logging.getLogger(__name__)


class OplogThread(threading.Thread):
    """Thread that tails an oplog.

    Calls the appropriate method on DocManagers for each relevant oplog entry.
    """
    def __init__(self, primary_client, doc_managers,
                 oplog_progress_dict, barrier=None, mongos_client=None, **kwargs):
        super(OplogThread, self).__init__()

        self.batch_size = kwargs.get('batch_size', DEFAULT_BATCH_SIZE)

        # The connection to the primary for this replicaSet.
        self.primary_client = primary_client

        # The connection to the mongos, if there is one.
        self.mongos_client = mongos_client

        # Are we allowed to perform a collection dump?
        self.initial_import = kwargs.get('initial_import', None)

        # The document manager for each target system.
        # These are the same for all threads.
        self.doc_managers = doc_managers

        # Boolean describing whether or not the thread is running.
        self.running = True

        # Stores the timestamp of the last oplog entry read.
        self.checkpoint = None

        # A dictionary that stores OplogThread/timestamp pairs.
        # Represents the last checkpoint for a OplogThread.
        self.oplog_progress = oplog_progress_dict

        # A semaphore implementation to synchronize initial data import
        self.barrier = barrier

        # The set of namespaces to process from the mongo cluster.
        self.namespace_set = kwargs.get('ns_set', [])

        # The set of gridfs namespaces to process from the mongo cluster
        self.gridfs_set = kwargs.get('gridfs_set', [])

        # The dict of source namespaces to destination namespaces
        self.dest_mapping = kwargs.get('dest_mapping', {})

        # Whether the collection dump gracefully handles exceptions
        self.continue_on_error = kwargs.get('continue_on_error', False)

        # Set of fields to export
        self.fields = kwargs.get('fields', {})

        LOG.info('OplogThread: Initializing oplog thread')

        self.oplog = self.primary_client.local.oplog.rs

        self.error_fields = {}

        if not self.oplog.find_one():
            err_msg = 'OplogThread: No oplog for thread:'
            LOG.warning('%s %s' % (err_msg, self.primary_connection))

    @property
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, value):
        self._fields = {}
        if value and 'include' in value and value['include']:
            self._fields['include'] = set(value['include'])
            # Always include _id field
            self._fields['include'].add('_id')
        elif value and 'exclude' in value and value['exclude']:
            self._fields['exclude'] = set(value['exclude'])
            # Never exclude _id field
            if('_id' in self._fields['exclude']):
                self._fields['exclude'].remove('_id')
        else:
            self._fields['include'] = None
            self._fields['exclude'] = None

    @property
    def initial_import(self):
        return self._initial_import

    @initial_import.setter
    def initial_import(self, initial_import):
        self._initial_import = {'dump': True, 'query': None}
        if initial_import:
            if 'dump' in initial_import:
                self._initial_import['dump'] = initial_import['dump']
            if 'query' in initial_import and initial_import['query']:
                self._initial_import['query'] = initial_import['query']

    @property
    def namespace_set(self):
        return self._namespace_set

    @namespace_set.setter
    def namespace_set(self, namespace_set):
        self._namespace_set = namespace_set
        self.update_oplog_ns_set()

    @property
    def gridfs_set(self):
        return self._gridfs_set

    @gridfs_set.setter
    def gridfs_set(self, gridfs_set):
        self._gridfs_set = gridfs_set
        self._gridfs_files_set = [ns + '.files' for ns in gridfs_set]
        self.update_oplog_ns_set()

    @property
    def gridfs_files_set(self):
        try:
            return self._gridfs_files_set
        except AttributeError:
            return []

    @property
    def oplog_ns_set(self):
        try:
            return self._oplog_ns_set
        except AttributeError:
            return []

    def update_oplog_ns_set(self):
        self._oplog_ns_set = []
        if self.namespace_set:
            self._oplog_ns_set.extend(self.namespace_set)
            self._oplog_ns_set.extend(self.gridfs_files_set)
            self._oplog_ns_set.extend(set(
                ns.split('.', 1)[0] + '.$cmd' for ns in self.namespace_set))
            self._oplog_ns_set.append("admin.$cmd")

    @log_fatal_exceptions
    def run(self):
        """Start the oplog worker.
        """
        LOG.debug("OplogThread: Run thread started")
        while self.running is True:
            LOG.debug("OplogThread: Getting cursor")
            cursor = None
            while not cursor:
                cursor = self.init_cursor()

            last_ts = None
            remove_inc = 0
            upsert_inc = 0
            update_inc = 0
            try:
                LOG.debug("OplogThread: about to process new oplog "
                          "entries")
                while cursor.alive and self.running:
                    LOG.debug("OplogThread: Cursor is still"
                              " alive and thread is still running.")
                    for n, entry in enumerate(cursor):

                        LOG.debug("OplogThread: Iterating through cursor,"
                                  " document number in this cursor is %d"
                                  % n)
                        # Break out if this thread should stop
                        if not self.running:
                            break

                        # Don't replicate entries resulting from chunk moves
                        if entry.get("fromMigrate"):
                            continue

                        # Take fields out of the oplog entry that
                        # shouldn't be replicated. This may nullify
                        # the document if there's nothing to do.
                        if not self.filter_oplog_entry(entry):
                            LOG.debug("OplogThread: Nullified entry: %r" % entry)
                            continue

                        # sync the current oplog operation
                        operation = entry['op']
                        ns = entry['ns']

                        if '.' not in ns:
                            continue
                        coll = ns.split('.', 1)[1]

                        # Ignore system collections
                        if coll.startswith("system."):
                            continue

                        # Ignore GridFS chunks
                        if coll.endswith('.chunks'):
                            continue

                        is_gridfs_file = False
                        if coll.endswith(".files"):
                            if ns in self.gridfs_files_set:
                                ns = ns[:-len(".files")]
                                is_gridfs_file = True
                            else:
                                continue

                        # use namespace mapping if one exists
                        namespace = self.dest_mapping.get(ns, ns)
                        timestamp = util.bson_ts_to_long(entry['ts'])
                        for docman in self.doc_managers:
                            try:
                                LOG.debug("OplogThread: Operation for this entry is %s and action is %r" % (str(operation), entry['o']))

                                # Remove
                                if operation == 'd':
                                    docman.remove(
                                        entry['o']['_id'], namespace, timestamp)
                                    remove_inc += 1

                                # Insert
                                elif operation == 'i':  # Insert
                                    # Retrieve inserted document from
                                    # 'o' field in oplog record
                                    doc = entry.get('o')
                                    # Extract timestamp and namespace
                                    if is_gridfs_file:
                                        db, coll = ns.split('.', 1)
                                        gridfile = GridFSFile(
                                            self.primary_client[db][coll],
                                            doc)
                                        docman.insert_file(
                                            gridfile, namespace, timestamp)
                                    else:
                                        self.upsert_doc(docman, ns, timestamp, doc['_id'], None, doc)
                                    upsert_inc += 1

                                # Update
                                elif operation == 'u':
                                    _id, error = docman.update(entry['o2']['_id'], entry['o'], namespace, timestamp)
                                    if error:
                                        if type(error) is NotFoundError:
                                            LOG.warning("Document with id: %s not found in Elastic Search, re-upserting the document" % _id)
                                            self.upsert_doc(docman, ns, timestamp, _id, None)
                                        else:
                                            LOG.warning("Failed to update document with id: %s, trying re-upsert" % _id)
                                            self.upsert_doc(docman, ns, timestamp, _id, error)
                                    update_inc += 1

                                # Command
                                elif operation == 'c':
                                    # use unmapped namespace
                                    doc = entry.get('o')
                                    docman.handle_command(doc,
                                                          entry['ns'],
                                                          timestamp)

                            except errors.OperationFailed:
                                LOG.exception(
                                    "Unable to process oplog document %r"
                                    % entry)
                            except errors.ConnectionFailed:
                                LOG.exception(
                                    "Connection failed while processing oplog "
                                    "document %r" % entry)
                            except Exception, e:
                                LOG.critical("Failed to process oplog document %r due to exception %r" % (entry, e))

                        if (remove_inc + upsert_inc + update_inc) is not 0 and (remove_inc + upsert_inc + update_inc) % 1000 == 0:
                            LOG.info(
                                "OplogThread: Documents removed: %d, "
                                "inserted: %d, updated: %d so far" % (
                                    remove_inc, upsert_inc, update_inc))

                        LOG.debug("OplogThread: Doc is processed.")

                        last_ts = entry['ts']

                        # update timestamp per batch size
                        # n % -1 (default for self.batch_size) == 0 for all n
                        if n % self.batch_size == 1 and last_ts is not None:
                            self.checkpoint = last_ts
                            self.update_checkpoint()

                    # update timestamp after running through oplog
                    if last_ts is not None:
                        LOG.debug("OplogThread: updating checkpoint after"
                                  "processing new oplog entries")
                        self.checkpoint = last_ts
                        self.update_checkpoint()

            except (pymongo.errors.AutoReconnect,
                    pymongo.errors.OperationFailure,
                    pymongo.errors.ConfigurationError):
                LOG.exception(
                    "Cursor closed due to an exception. "
                    "Will attempt to reconnect.")

            # update timestamp before attempting to reconnect to MongoDB,
            # after being join()'ed, or if the cursor closes
            if last_ts is not None:
                LOG.debug("OplogThread: updating checkpoint after an "
                          "Exception, cursor closing, or join() on this"
                          "thread.")
                self.checkpoint = last_ts
                self.update_checkpoint()

            LOG.debug("OplogThread: Sleeping. Documents removed: %d, "
                      "upserted: %d, updated: %d"
                      % (remove_inc, upsert_inc, update_inc))
            time.sleep(2)

    def join(self):
        """Stop this thread from managing the oplog.
        """
        LOG.warning('Fields with errors: %r' % self.error_fields)
        LOG.info("OplogThread: exiting due to join call.")
        self.running = False
        threading.Thread.join(self)

    def pop_excluded_fields(self, doc):
        if 'exclude' in self._fields and self._fields['exclude']:
            for key in self._fields['exclude']:
                doc.pop(key, None)
        elif 'include' in self._fields and self._fields['include']:
            for key in set(doc) - set(self._fields['include']):
                doc.pop(key, None)

    def filter_oplog_entry(self, entry):
        """Remove fields from an oplog entry that should not be replicated."""
        if not self._fields or ('include' not in self._fields and 'exclude' not in self._fields):
            return entry

        entry_o = entry['o']
        # 'i' indicates an insert. 'o' field is the doc to be inserted.
        if entry['op'] == 'i':
            self.pop_excluded_fields(entry_o)
        # 'u' indicates an update. The 'o' field describes an update spec
        # if '$set' or '$unset' are present.
        elif entry['op'] == 'u' and ('$set' in entry_o or '$unset' in entry_o):
            self.pop_excluded_fields(entry_o.get("$set", {}))
            self.pop_excluded_fields(entry_o.get("$unset", {}))
            # not allowed to have empty $set/$unset, so remove if empty
            if "$set" in entry_o and not entry_o['$set']:
                entry_o.pop("$set")
            if "$unset" in entry_o and not entry_o['$unset']:
                entry_o.pop("$unset")
            if not entry_o:
                return None
        # 'u' indicates an update. The 'o' field is the replacement document
        # if no '$set' or '$unset' are present.
        elif entry['op'] == 'u':
            self.pop_excluded_fields(entry_o)

        return entry

    def get_oplog_cursor(self, timestamp=None):
        """Get a cursor to the oplog after the given timestamp, filtering
        entries not in the namespace set.
        If no timestamp is specified, returns a cursor to the entire oplog.
        """
        query = {}
        if self.oplog_ns_set:
            query['ns'] = {'$in': self.oplog_ns_set}

        if timestamp is None:
            cursor = self.oplog.find(
                query,
                tailable=True, await_data=True)
        else:
            query['ts'] = {'$gte': timestamp}
            cursor = self.oplog.find(
                query, tailable=True, await_data=True)
            # Applying 8 as the mask to the cursor enables OplogReplay
            cursor.add_option(8)
        LOG.info("Created oplog cursor with timestamp: %r" % timestamp)
        return cursor

    def get_failed_doc(self, namespace, doc_id):
        attempts = 0
        database, coll = namespace.split('.', 1)
        while attempts <= 60:
            try:
                target_coll = self.primary_client[database][coll]
                fields_to_fetch = None
                if 'include' in self._fields and len(self._fields['include']) > 0:
                    fields_to_fetch = self._fields['include']
                doc = util.retry_until_ok(
                    target_coll.find_one,
                    {"_id": ObjectId(doc_id)},
                    fields=fields_to_fetch
                )
                if doc:
                    self.pop_excluded_fields(doc)
                    LOG.info("Reinserting failed document with id: %r" % doc_id)
                else:
                    LOG.critical("Could not find document with id %s from mongodb", doc_id)
                break
            except pymongo.errors.AutoReconnect, pymongo.errors.OperationFailure:
                LOG.warning("MongoDB possible AutoReconnect, retrying in 1s")
                attempts += 1
                time.sleep(1)
        if attempts >= 60:
            LOG.critical("Coudnt retrieve document with id %s from mongo after %d attempts" % (doc_id, attempts))
        return doc

    def remove_field_from_doc(self, doc, field):
        if '.' in field:
            dict_key = field.split('.', 1)
            if dict_key[0] in doc:
                self.remove_field_from_doc(doc[dict_key[0]], dict_key[1])
            else:
                self.remove_field_from_doc(doc, dict_key[1])
        else:
            try:
                doc.pop(field)
            except Exception:
                LOG.warning("Error Field [%r] not found in doc: %r" % (field, doc))

    def upsert_doc(self, dm, namespace, ts, _id, error_field, doc=None, retry_count=0):
        mapped_ns = self.dest_mapping.get(namespace, namespace)
        doc_to_upsert = doc
        if not doc_to_upsert and _id:
            doc_to_upsert = self.get_failed_doc(namespace, _id)
        if error_field:
            self.error_fields[error_field] = self.error_fields.get(error_field, 0) + 1
            self.remove_field_from_doc(doc_to_upsert, error_field)
        if retry_count > 50:
            raise Exception("Hit maximum retry attempts when trying to insert document: %r" % doc_to_upsert)
        try:
            if doc_to_upsert:
                error = dm.upsert(doc_to_upsert, mapped_ns)
                if error:
                    self.upsert_doc(dm, namespace, ts, error[0], error[1], doc_to_upsert, retry_count+1)
            else:
                LOG.error("Empty Document found: %r" % _id)
            # self._fields['exclude'].remove(field)
        except Exception, e:
            LOG.critical("Failed to upsert document: %r due to error: %r" % (doc_to_upsert, e))
            raise

    def get_dump_set(self):
        dump_set = self.namespace_set or []

        # no namespaces specified
        if not self.namespace_set:
            db_list = retry_until_ok(self.primary_client.database_names)
            for database in db_list:
                if database == "config" or database == "local":
                    continue
                coll_list = retry_until_ok(
                    self.primary_client[database].collection_names)
                for coll in coll_list:
                    # ignore system collections
                    if coll.startswith("system."):
                        continue
                    # ignore gridfs collections
                    if coll.endswith(".files") or coll.endswith(".chunks"):
                        continue
                    namespace = "%s.%s" % (database, coll)
                    dump_set.append(namespace)
        return dump_set

    def dump_collection(self):
        """Dumps collection into the target system.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """

        dump_set = self.get_dump_set()

        LOG.info("OplogThread: Dumping set of collections %s " % dump_set)
        timestamp = util.retry_until_ok(self.get_last_oplog_timestamp)
        if timestamp is None:
            return None
        long_ts = util.bson_ts_to_long(timestamp)

        def query_handle_id(query):
            if '_id' in query and query['_id']:
                for key in query['_id']:
                    query['_id'][key] = ObjectId(query['_id'][key])

        def docs_to_dump(namespace):
            database, coll = namespace.split('.', 1)
            last_id = None
            attempts = 0

            # Loop to handle possible AutoReconnect
            while attempts < 60:
                target_coll = self.primary_client[database][coll]
                fields_to_fetch = None
                if 'include' in self._fields:
                    fields_to_fetch = self._fields['include']
                query = {}
                if self.initial_import['query']:
                    query_handle_id(self.initial_import['query'])
                    query = self.initial_import['query']
                if not last_id:
                    cursor = util.retry_until_ok(
                        target_coll.find,
                        query,
                        fields=fields_to_fetch,
                        sort=[("_id", pymongo.ASCENDING)]
                    )
                else:
                    cursor = util.retry_until_ok(
                        target_coll.find,
                        query.update({"_id": {"$gt": last_id}}),
                        fields=fields_to_fetch,
                        sort=[("_id", pymongo.ASCENDING)]
                    )
                try:
                    for doc in cursor:
                        if not self.running:
                            LOG.error("Stopped iterating over Cursor while initial import")
                            raise StopIteration
                        self.pop_excluded_fields(doc)
                        last_id = doc["_id"]
                        yield doc
                    break
                except (pymongo.errors.AutoReconnect,
                        pymongo.errors.OperationFailure):
                    attempts += 1
                    time.sleep(1)

        def upsert_all_failed_docs(dm, namespace, errors):
            for _id, field in errors:
                self.upsert_doc(dm, namespace, long_ts, _id, field)

        def upsert_each(dm):
            num_inserted = 0
            num_failed = 0
            for namespace in dump_set:
                mapped_ns = self.dest_mapping.get(namespace, namespace)
                dm.disable_refresh(mapped_ns)
                for num, doc in enumerate(docs_to_dump(namespace)):
                    if num % 10000 == 0:
                        LOG.info("Upserted %d docs." % num)
                    try:
                        self.upsert_doc(dm, namespace, long_ts, None, None, doc)
                        num_inserted += 1
                    except Exception:
                        if self.continue_on_error:
                            LOG.exception("Could not upsert document: %r" % doc)
                            num_failed += 1
                        else:
                            raise
                dm.enable_refresh(mapped_ns)
            LOG.info("Upserted %d docs" % num_inserted)
            if num_failed > 0:
                LOG.error("Failed to upsert %d docs" % num_failed)

        def upsert_all(dm):
            try:
                for namespace in dump_set:
                    mapped_ns = self.dest_mapping.get(namespace, namespace)
                    dm.disable_refresh(mapped_ns)
                    errors = dm.bulk_upsert(docs_to_dump(namespace), mapped_ns, long_ts)
                    upsert_all_failed_docs(dm, namespace, errors)
                    dm.enable_refresh(mapped_ns)
            except Exception:
                if self.continue_on_error:
                    LOG.exception("OplogThread: caught exception"
                                  " during bulk upsert, re-upserting"
                                  " documents serially")
                    upsert_each(dm)
                else:
                    raise

        def do_dump(dm, error_queue):
            try:
                # Dump the documents, bulk upsert if possible
                if hasattr(dm, "bulk_upsert"):
                    LOG.debug("OplogThread: Using bulk upsert function for "
                              "collection dump")
                    upsert_all(dm)
                else:
                    LOG.debug(
                        "OplogThread: DocManager %s has no "
                        "bulk_upsert method.  Upserting documents "
                        "serially for collection dump." % str(dm))
                    upsert_each(dm)

                # Dump GridFS files
                for gridfs_ns in self.gridfs_set:
                    db, coll = gridfs_ns.split('.', 1)
                    mongo_coll = self.primary_client[db][coll]
                    dest_ns = self.dest_mapping.get(gridfs_ns, gridfs_ns)
                    for doc in docs_to_dump(gridfs_ns + '.files'):
                        gridfile = GridFSFile(mongo_coll, doc)
                        dm.insert_file(gridfile, dest_ns, long_ts)
            except:
                # Likely exceptions:
                # pymongo.errors.OperationFailure,
                # mongo_connector.errors.ConnectionFailed
                # mongo_connector.errors.OperationFailed
                error_queue.put(sys.exc_info())

        # Extra threads (if any) that assist with collection dumps
        dumping_threads = []
        # Did the dump succeed for all target systems?
        dump_success = True
        # Holds any exceptions we can't recover from
        errors = queue.Queue()

        if len(self.doc_managers) == 1:
            do_dump(self.doc_managers[0], errors)
        else:
            # Slight performance gain breaking dump into separate
            # threads if > 1 replication target
            for dm in self.doc_managers:
                t = threading.Thread(target=do_dump, args=(dm, errors))
                dumping_threads.append(t)
                t.start()
            # cleanup
            for t in dumping_threads:
                t.join()

        # Print caught exceptions
        try:
            while True:
                LOG.critical('Exception during collection dump',
                             exc_info=errors.get_nowait())
                dump_success = False
        except queue.Empty:
            pass

        if not dump_success:
            err_msg = "OplogThread: Failed during dump collection"
            effect = "cannot recover!"
            LOG.error('%s %s %s' % (err_msg, effect, self.oplog))
            self.running = False
            return None
        else:
            LOG.info('OplogThread: Successfully dumped collection')
            LOG.warning('Fields with errors: %r' % self.error_fields)
            LOG.info("Waiting for other threads to finish collection dump")
            self.barrier.wait()

        return timestamp

    def get_last_oplog_timestamp(self):
        """Return the timestamp of the latest entry in the oplog.
        """
        if not self.oplog_ns_set:
            curr = self.oplog.find().sort(
                '$natural', pymongo.DESCENDING
            ).limit(1)
        else:
            curr = self.oplog.find(
                {'ns': {'$in': self.oplog_ns_set}}
            ).sort('$natural', pymongo.DESCENDING).limit(1)

        if curr.count(with_limit_and_skip=True) == 0:
            return None

        LOG.debug("OplogThread: Last oplog entry has timestamp %d."
                  % curr[0]['ts'].time)
        return curr[0]['ts']

    def init_cursor(self):
        """Position the cursor appropriately.

        The cursor is set to either the beginning of the oplog, or
        wherever it was last left off.

        Returns the cursor and the number of documents left in the cursor.
        """
        timestamp = self.read_last_checkpoint()

        LOG.info("Initializing cursor with initial timestamp: %r" % timestamp)

        dump_set = self.get_dump_set()

        for dm in self.doc_managers:
            for namespace in dump_set:
                mapped_ns = self.dest_mapping.get(namespace, namespace)
                if not dm.index_exists(mapped_ns):
                    dm.index_create(mapped_ns)

        if timestamp is None:
            if self.initial_import['dump']:
                # dump collection and update checkpoint
                LOG.info("INITIAL IMPORT: Starting initial import of data")
                timestamp = self.dump_collection()
                if timestamp is None:
                    return None
                for dm in self.doc_managers:
                    for namespace in dump_set:
                        mapped_ns = self.dest_mapping.get(namespace, namespace)
                        dm.index_alias_add(mapped_ns)
            else:
                # Collection dump disabled:
                # return cursor to beginning of oplog.
                LOG.info("INITIAL IMPORT: Initial import skipped, creating oplog cursor")
                cursor = self.get_oplog_cursor()
                self.checkpoint = self.get_last_oplog_timestamp()
                self.update_checkpoint()
                return cursor
        else:
            LOG.info("Last checkpoint found from timestamp file, resuming oplog tailing from timestamp: %r" % timestamp)

        self.checkpoint = timestamp
        self.update_checkpoint()

        for i in range(60):
            cursor = self.get_oplog_cursor(timestamp)

            # try to get the first oplog entry
            try:
                first_oplog_entry = retry_until_ok(next, cursor)
            except StopIteration:
                # It's possible for the cursor to become invalid
                # between the cursor.count() call and now
                time.sleep(1)
                continue

            # first entry should be last oplog entry processed
            LOG.info("Retrieved first oplog entry: %r" % first_oplog_entry)
            cursor_ts_long = util.bson_ts_to_long(
                first_oplog_entry.get("ts"))
            given_ts_long = util.bson_ts_to_long(timestamp)
            if cursor_ts_long > given_ts_long:
                # first entry in oplog is beyond timestamp
                # we've fallen behind
                LOG.critical("Oplog Cursor has fallen behind, reimporting data")
                self.clear_checkpoint()
                self.barrier = Barrier(1)
                self.initial_import['dump'] = True
                return None

            # first entry has been consumed
            LOG.info("Created oplog cursor")
            return cursor

        else:
            raise errors.MongoConnectorError(
                "Could not initialize oplog cursor.")

    def update_checkpoint(self):
        """Store the current checkpoint in the oplog progress dictionary.
        """
        if self.checkpoint is not None:
            with self.oplog_progress as oplog_prog:
                oplog_dict = oplog_prog.get_dict()
                oplog_dict[str(self.oplog)] = self.checkpoint
                LOG.debug("OplogThread: oplog checkpoint updated to %s" %
                          str(self.checkpoint))
        else:
            LOG.debug("OplogThread: no checkpoint to update.")

    def clear_checkpoint(self):
        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            oplog_dict.pop(str(self.oplog))
            LOG.info("OplogThread: Cleared checkpoint to initiate reimport")

    def read_last_checkpoint(self):
        """Read the last checkpoint from the oplog progress dictionary.
        """
        oplog_str = str(self.oplog)
        ret_val = None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            if oplog_str in oplog_dict.keys():
                ret_val = oplog_dict[oplog_str]

        LOG.info("OplogThread: reading last checkpoint as %s " % str(ret_val))
        return ret_val
