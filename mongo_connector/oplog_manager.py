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
import sys
import time
import threading
import re

import pymongo

from pymongo import CursorType

from mongo_connector import errors, util
from mongo_connector.constants import DEFAULT_BATCH_SIZE
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector.util import log_fatal_exceptions, retry_until_ok

LOG = logging.getLogger(__name__)


class OplogThread(threading.Thread):
    """Thread that tails an oplog.

    Calls the appropriate method on DocManagers for each relevant oplog entry.
    """
    def __init__(self, primary_client, doc_managers,
                 oplog_progress_dict, dest_mapping_stru,
                 mongos_client=None, **kwargs):
        super(OplogThread, self).__init__()

        self.batch_size = kwargs.get('batch_size', DEFAULT_BATCH_SIZE)

        # The connection to the primary for this replicaSet.
        self.primary_client = primary_client

        # The connection to the mongos, if there is one.
        self.mongos_client = mongos_client

        # Are we allowed to perform a collection dump?
        self.collection_dump = kwargs.get('collection_dump', True)

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

        # The set of namespaces to process from the mongo cluster.
        self.namespace_set = kwargs.get('ns_set', [])

        # The set of namespaces not to process from the mongo cluster.
        self.ex_namespace_set = kwargs.get('ex_ns_set', [])

        # The set of gridfs namespaces to process from the mongo cluster
        self.gridfs_set = kwargs.get('gridfs_set', [])

        # The dict of source namespaces to destination namespaces
        self.dest_mapping = kwargs.get('dest_mapping', {})

        # The structure handling dest_mappings dynamically
        self.dest_mapping_stru = dest_mapping_stru

        # Whether the collection dump gracefully handles exceptions
        self.continue_on_error = kwargs.get('continue_on_error', False)

        # Set of fields to export
        self._exclude_fields = set([])
        self.fields = kwargs.get('fields', None)
        if kwargs.get('exclude_fields', None):
            self.exclude_fields = kwargs['exclude_fields']

        LOG.info('OplogThread: Initializing oplog thread')

        self.oplog = self.primary_client.local.oplog.rs
        self.replset_name = (
            self.primary_client.admin.command('ismaster')['setName'])

        if not self.oplog.find_one():
            err_msg = 'OplogThread: No oplog for thread:'
            LOG.warning('%s %s' % (err_msg, self.primary_connection))

    @property
    def fields(self):
        if self._fields:
            return list(self._fields)
        return None

    @property
    def exclude_fields(self):
        if self._exclude_fields:
            return list(self._exclude_fields)
        return None

    @fields.setter
    def fields(self, value):
        if self._exclude_fields:
            raise errors.InvalidConfiguration(
                "Cannot set 'fields' when 'exclude_fields' has already "
                "been set to non-empty list.")
        if value:
            self._fields = set(value)
            # Always include _id field
            self._fields.add('_id')
            self._projection = dict((field, 1) for field in self._fields)
        else:
            self._fields = set([])
            self._projection = None

    @exclude_fields.setter
    def exclude_fields(self, value):
        if self._fields:
            raise errors.InvalidConfiguration(
                "Cannot set 'exclude_fields' when 'fields' has already "
                "been set to non-empty list.")
        if value:
            self._exclude_fields = set(value)
            if '_id' in value:
                LOG.warning("OplogThread: Cannot exclude '_id' field, "
                            "ignoring")
                self._exclude_fields.remove('_id')
            if not self._exclude_fields:
                self._projection = None
            else:
                self._projection = dict(
                    (field, 0) for field in self._exclude_fields)
        else:
            self._exclude_fields = set([])
            self._projection = None

    @property
    def namespace_set(self):
        return self._namespace_set

    @namespace_set.setter
    def namespace_set(self, namespace_set):
        self._namespace_set = namespace_set
        self.update_oplog_ns_set()

    @property
    def ex_namespace_set(self):
        return self._ex_namespace_set

    @ex_namespace_set.setter
    def ex_namespace_set(self, ex_namespace_set):
        self._ex_namespace_set = ex_namespace_set
        self.update_oplog_ex_ns_set()

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
            for ns in self.namespace_set:
                self._oplog_ns_set.append(ns)
                self._oplog_ns_set.append(ns.split('.', 1)[0] + '.$cmd')

            self._oplog_ns_set.extend(self.gridfs_files_set)
            self._oplog_ns_set.append("admin.$cmd")

    @property
    def oplog_ex_ns_set(self):
        return self._oplog_ex_ns_set

    def update_oplog_ex_ns_set(self):
        self._oplog_ex_ns_set = []
        if self.ex_namespace_set:
            for ens in self.ex_namespace_set:
                self._oplog_ex_ns_set.append(ens)

    @log_fatal_exceptions
    def run(self):
        """Start the oplog worker.
        """
        LOG.debug("OplogThread: Run thread started")
        while self.running is True:
            LOG.debug("OplogThread: Getting cursor")
            cursor, cursor_empty = self.init_cursor()

            # we've fallen too far behind
            if cursor is None and self.checkpoint is not None:
                err_msg = "OplogThread: Last entry no longer in oplog"
                effect = "cannot recover!"
                LOG.error('%s %s %s' % (err_msg, effect, self.oplog))
                self.running = False
                continue

            if cursor_empty:
                LOG.debug("OplogThread: Last entry is the one we "
                          "already processed.  Up to date.  Sleeping.")
                time.sleep(1)
                continue

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
                            continue

                        # Sync the current oplog operation
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

                        # Ignore the collection if it is not included
                        # In the connector.py we already verified that
                        # it is not possible
                        # to have include and exclude in the same time.
                        if self.oplog_ns_set and not self.oplog_ex_ns_set and not self.dest_mapping_stru.match_set(ns, self.oplog_ns_set):
                            LOG.debug("OplogThread: Skipping oplog entry: "
                                      "'%s' is not in the namespace set." %
                                      (ns,))
                            continue
                        if self.oplog_ex_ns_set and not self.oplog_ns_set and self.dest_mapping_stru.match_set(ns, self.oplog_ex_ns_set):
                            LOG.debug("OplogThread: Skipping oplog entry: "
                                      "'%s' is in the exclude namespace set." %
                                      (ns,))
                            continue

                        # use namespace mapping if one exists
                        ns = self.dest_mapping_stru.get(ns, ns)
                        if ns is None:
                            LOG.debug("OplogThread: Skipping oplog entry: "
                                      "'%s' is not in the namespace set." %
                                      (ns,))
                            continue
                        timestamp = util.bson_ts_to_long(entry['ts'])
                        for docman in self.doc_managers:
                            try:
                                LOG.debug("OplogThread: Operation for this "
                                          "entry is %s" % str(operation))

                                # Remove
                                if operation == 'd':
                                    docman.remove(
                                        entry['o']['_id'], ns, timestamp)
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
                                            gridfile, ns, timestamp)
                                    else:
                                        docman.upsert(doc, ns, timestamp)
                                    upsert_inc += 1

                                # Update
                                elif operation == 'u':
                                    docman.update(entry['o2']['_id'],
                                                  entry['o'],
                                                  ns, timestamp)
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

                        if (remove_inc + upsert_inc + update_inc) % 1000 == 0:
                            LOG.debug(
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
        LOG.debug("OplogThread: exiting due to join call.")
        self.running = False
        threading.Thread.join(self)

    @classmethod
    def _find_field(cls, field, doc):
        """Find the field in the document which matches the given field.

        The field may be in dot notation, eg "a.b.c". Returns a list with
        a single tuple (path, field_value) or the empty list if the field
        is not present.
        """
        path = field.split('.')
        try:
            for key in path:
                doc = doc[key]
            return [(path, doc)]
        except (KeyError, TypeError):
            return []

    @classmethod
    def _find_update_fields(cls, field, doc):
        """Find the fields in the update document which match the given field.

        Both the field and the top level keys in the doc may be in dot
        notation, eg "a.b.c". Returns a list of tuples (path, field_value) or
        the empty list if the field is not present.
        """
        def find_partial_matches():
            for key in doc:
                if len(key) > len(field):
                    # Handle case where field is a prefix of key, eg field is
                    # 'a' and key is 'a.b'.
                    if key.startswith(field) and key[len(field)] == '.':
                        yield [key], doc[key]
                        # Continue searching, there may be multiple matches.
                        # For example, field 'a' should match 'a.b' and 'a.c'.
                elif len(key) < len(field):
                    # Handle case where key is a prefix of field, eg field is
                    # 'a.b' and key is 'a'.
                    if field.startswith(key) and field[len(key)] == '.':
                        # Search for the remaining part of the field
                        matched = cls._find_field(field[len(key) + 1:],
                                                  doc[key])
                        if matched:
                            # Add the top level key to the path.
                            match = matched[0]
                            match[0].insert(0, key)
                            yield match
                        # Stop searching, it's not possible for any other
                        # keys in the update doc to match this field.
                        return

        try:
            return [([field], doc[field])]
        except KeyError:
            # Field does not exactly match any key in the update doc.
            return list(find_partial_matches())

    def _pop_excluded_fields(self, doc, update=False):
        # Remove all the fields that were passed in exclude_fields.
        find_fields = self._find_update_fields if update else self._find_field
        for field in self._exclude_fields:
            for path, _ in find_fields(field, doc):
                # Delete each matching field in the original document.
                temp_doc = doc
                for p in path[:-1]:
                    temp_doc = temp_doc[p]
                temp_doc.pop(path[-1])

        return doc  # Need this to be similar to copy_included_fields.

    def _copy_included_fields(self, doc, update=False):
        new_doc = {}
        find_fields = self._find_update_fields if update else self._find_field
        for field in self._fields:
            for path, value in find_fields(field, doc):
                # Copy each matching field in the original document.
                temp_doc = new_doc
                for p in path[:-1]:
                    temp_doc = temp_doc.setdefault(p, {})
                temp_doc[path[-1]] = value

        return new_doc

    def filter_oplog_entry(self, entry):
        """Remove fields from an oplog entry that should not be replicated.

        NOTE: this does not support array indexing, for example 'a.b.2'"""
        if not self._fields and not self._exclude_fields:
            return entry
        elif self._fields:
            filter_fields = self._copy_included_fields
        else:
            filter_fields = self._pop_excluded_fields

        entry_o = entry['o']
        # 'i' indicates an insert. 'o' field is the doc to be inserted.
        if entry['op'] == 'i':
            entry['o'] = filter_fields(entry_o)
        # 'u' indicates an update. The 'o' field describes an update spec
        # if '$set' or '$unset' are present.
        elif entry['op'] == 'u' and ('$set' in entry_o or '$unset' in entry_o):
            if '$set' in entry_o:
                entry['o']["$set"] = filter_fields(entry_o["$set"],
                                                   update=True)
            if '$unset' in entry_o:
                entry['o']["$unset"] = filter_fields(entry_o["$unset"],
                                                     update=True)
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
            entry['o'] = filter_fields(entry_o)

        return entry

    def get_oplog_cursor(self, timestamp=None):
        """Get a cursor to the oplog after the given timestamp, excluding
        no-op entries.

        If no timestamp is specified, returns a cursor to the entire oplog.
        """
        query = {'op': {'$ne': 'n'}}
        if timestamp is None:
            cursor = self.oplog.find(
                query,
                cursor_type=CursorType.TAILABLE_AWAIT)
        else:
            query['ts'] = {'$gte': timestamp}
            cursor = self.oplog.find(
                query,
                cursor_type=CursorType.TAILABLE_AWAIT)
            # Applying 8 as the mask to the cursor enables OplogReplay
            cursor.add_option(8)
        return cursor

    def dump_collection(self):
        """Dumps collection into the target system.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """

        timestamp = util.retry_until_ok(self.get_last_oplog_timestamp)
        if timestamp is None:
            return None
        long_ts = util.bson_ts_to_long(timestamp)

        def get_all_ns():
            all_ns_set = []
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
                    all_ns_set.append(namespace)
            return all_ns_set

        def get_ns_from_set(namespace_set, include=True):
            all_ns_set = get_all_ns()
            ns_set = []
            for src_ns in all_ns_set:
                for map_ns in namespace_set:
                    # get all matched collections in that ns
                    reg_pattern = r'\A' + map_ns.replace('*', '(.*)') + r'\Z'
                    if re.match(reg_pattern, src_ns):
                        ns_set.append(src_ns)
                        continue

            if include:
                return ns_set
            else:
                return [x for x in all_ns_set if x not in ns_set]

        # No namespaces specified
        if (not self.namespace_set) and (not self.ex_namespace_set):
            dump_set = get_all_ns()
        elif self.namespace_set and not self.ex_namespace_set:
            dump_set = get_ns_from_set(self.namespace_set)
        else:
            # ex_namespace_set is set but no namespace_set
            dump_set = get_ns_from_set(self.ex_namespace_set, include=False)

        LOG.debug("OplogThread: Dumping set of collections %s " % dump_set)

        def docs_to_dump(namespace):
            database, coll = namespace.split('.', 1)
            last_id = None
            attempts = 0

            # Loop to handle possible AutoReconnect
            while attempts < 60:
                target_coll = self.primary_client[database][coll]
                if not last_id:
                    cursor = util.retry_until_ok(
                        target_coll.find,
                        projection=self._projection,
                        sort=[("_id", pymongo.ASCENDING)]
                    )
                else:
                    cursor = util.retry_until_ok(
                        target_coll.find,
                        {"_id": {"$gt": last_id}},
                        projection=self._projection,
                        sort=[("_id", pymongo.ASCENDING)]
                    )
                try:
                    for doc in cursor:
                        if not self.running:
                            raise StopIteration
                        last_id = doc["_id"]
                        yield doc
                    break
                except (pymongo.errors.AutoReconnect,
                        pymongo.errors.OperationFailure):
                    attempts += 1
                    time.sleep(1)

        def upsert_each(dm):
            num_inserted = 0
            num_failed = 0
            for namespace in dump_set:
                for num, doc in enumerate(docs_to_dump(namespace)):
                    if num % 10000 == 0:
                        LOG.debug("Upserted %d docs." % num)
                    try:
                        mapped_ns = self.dest_mapping_stru.get(namespace,
                                                               namespace)
                        dm.upsert(doc, mapped_ns, long_ts)
                        num_inserted += 1
                    except Exception:
                        if self.continue_on_error:
                            LOG.exception(
                                "Could not upsert document: %r" % doc)
                            num_failed += 1
                        else:
                            raise
            LOG.debug("Upserted %d docs" % num_inserted)
            if num_failed > 0:
                LOG.error("Failed to upsert %d docs" % num_failed)

        def upsert_all(dm):
            try:
                for namespace in dump_set:
                    mapped_ns = self.dest_mapping_stru.get(namespace,
                                                           namespace)
                    dm.bulk_upsert(docs_to_dump(namespace), mapped_ns, long_ts)
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
                    dest_ns = self.dest_mapping_stru.get(gridfs_ns, gridfs_ns)
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

        return timestamp

    def _get_oplog_timestamp(self, newest_entry):
        """Return the timestamp of the latest or earliest entry in the oplog.
        """
        sort_order = pymongo.DESCENDING if newest_entry else pymongo.ASCENDING
        curr = self.oplog.find({'op': {'$ne': 'n'}}).sort(
                '$natural', sort_order
            ).limit(-1)

        try:
            ts = next(curr)['ts']
        except StopIteration:
            LOG.debug("OplogThread: oplog is empty.")
            return None

        LOG.debug("OplogThread: %s oplog entry has timestamp %d."
                  % ('Newest' if newest_entry else 'Oldest', ts.time))
        return ts

    def get_oldest_oplog_timestamp(self):
        """Return the timestamp of the oldest entry in the oplog.
        """
        return self._get_oplog_timestamp(False)

    def get_last_oplog_timestamp(self):
        """Return the timestamp of the newest entry in the oplog.
        """
        return self._get_oplog_timestamp(True)

    def _cursor_empty(self, cursor):
        try:
            next(cursor.clone().limit(-1))
            return False
        except StopIteration:
            return True

    def init_cursor(self):
        """Position the cursor appropriately.

        The cursor is set to either the beginning of the oplog, or
        wherever it was last left off.

        Returns the cursor and True if the cursor is empty.
        """
        timestamp = self.read_last_checkpoint()

        if timestamp is None:
            if self.collection_dump:
                # dump collection and update checkpoint
                timestamp = self.dump_collection()
                if timestamp is None:
                    return None, True
            else:
                # Collection dump disabled:
                # return cursor to beginning of oplog.
                cursor = self.get_oplog_cursor()
                self.checkpoint = self.get_last_oplog_timestamp()
                self.update_checkpoint()
                return cursor, retry_until_ok(self._cursor_empty, cursor)

        self.checkpoint = timestamp
        self.update_checkpoint()

        for i in range(60):
            cursor = self.get_oplog_cursor(timestamp)
            cursor_empty = retry_until_ok(self._cursor_empty, cursor)

            if cursor_empty:
                # rollback, update checkpoint, and retry
                LOG.debug("OplogThread: Initiating rollback from "
                          "get_oplog_cursor")
                self.checkpoint = self.rollback()
                self.update_checkpoint()
                return self.init_cursor()

            # try to get the first oplog entry
            try:
                first_oplog_entry = retry_until_ok(next, cursor)
            except StopIteration:
                # It's possible for the cursor to become invalid
                # between the next(cursor) call and now
                time.sleep(1)
                continue

            oldest_ts_long = util.bson_ts_to_long(
                self.get_oldest_oplog_timestamp())
            checkpoint_ts_long = util.bson_ts_to_long(timestamp)
            if checkpoint_ts_long < oldest_ts_long:
                # We've fallen behind, the checkpoint has fallen off the oplog
                return None, True

            cursor_ts_long = util.bson_ts_to_long(first_oplog_entry["ts"])
            if cursor_ts_long > checkpoint_ts_long:
                # The checkpoint is not present in this oplog and the oplog
                # did not rollover. This means that we connected to a new
                # primary which did not replicate the checkpoint and which has
                # new changes in its oplog for us to process.
                # rollback, update checkpoint, and retry
                LOG.debug("OplogThread: Initiating rollback from "
                          "get_oplog_cursor: new oplog entries found but "
                          "checkpoint is not present")
                self.checkpoint = self.rollback()
                self.update_checkpoint()
                return self.init_cursor()

            # first entry has been consumed
            return cursor, cursor_empty

        else:
            raise errors.MongoConnectorError(
                "Could not initialize oplog cursor.")

    def update_checkpoint(self):
        """Store the current checkpoint in the oplog progress dictionary.
        """
        if self.checkpoint is not None:
            with self.oplog_progress as oplog_prog:
                oplog_dict = oplog_prog.get_dict()
                # If we have the repr of our oplog collection
                # in the dictionary, remove it and replace it
                # with our replica set name.
                # This allows an easy upgrade path from mongo-connector 2.3.
                # For an explanation of the format change, see the comment in
                # read_last_checkpoint.
                oplog_dict.pop(str(self.oplog), None)
                oplog_dict[self.replset_name] = self.checkpoint
                LOG.debug("OplogThread: oplog checkpoint updated to %s" %
                          str(self.checkpoint))
        else:
            LOG.debug("OplogThread: no checkpoint to update.")

    def read_last_checkpoint(self):
        """Read the last checkpoint from the oplog progress dictionary.
        """
        # In versions of mongo-connector 2.3 and before,
        # we used the repr of the
        # oplog collection as keys in the oplog_progress dictionary.
        # In versions thereafter, we use the replica set name. For backwards
        # compatibility, we check for both.
        oplog_str = str(self.oplog)

        ret_val = None
        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
            try:
                # New format.
                ret_val = oplog_dict[self.replset_name]
            except KeyError:
                try:
                    # Old format.
                    ret_val = oplog_dict[oplog_str]
                except KeyError:
                    pass

        LOG.debug("OplogThread: reading last checkpoint as %s " %
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
        LOG.debug("OplogThread: Initiating rollback sequence to bring "
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

        LOG.debug("OplogThread: last oplog entry is %s"
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
                original_namespace = self.dest_mapping_stru.get_key(namespace)
                if not original_namespace:
                    original_namespace = namespace

                database, coll = original_namespace.split('.', 1)
                obj_id = bson.objectid.ObjectId
                bson_obj_id_list = [obj_id(doc['_id']) for doc in doc_list]

                # Use connection to whole cluster if in sharded environment.
                client = self.mongos_client or self.primary_client
                to_update = util.retry_until_ok(
                    client[database][coll].find,
                    {'_id': {'$in': bson_obj_id_list}},
                    projection=self._projection
                )
                # Doc list are docs in target system, to_update are
                # Docs in mongo
                doc_hash = {}  # Hash by _id
                for doc in doc_list:
                    doc_hash[bson.objectid.ObjectId(doc['_id'])] = doc

                to_index = []

                def collect_existing_docs():
                    for doc in to_update:
                        if doc['_id'] in doc_hash:
                            del doc_hash[doc['_id']]
                            to_index.append(doc)
                retry_until_ok(collect_existing_docs)

                # Delete the inconsistent documents
                LOG.debug("OplogThread: Rollback, removing inconsistent "
                          "docs.")
                remov_inc = 0
                for document_id in doc_hash:
                    try:
                        dm.remove(document_id, namespace,
                                  util.bson_ts_to_long(rollback_cutoff_ts))
                        remov_inc += 1
                        LOG.debug(
                            "OplogThread: Rollback, removed %r " % doc)
                    except errors.OperationFailed:
                        LOG.warning(
                            "Could not delete document during rollback: %r "
                            "This can happen if this document was already "
                            "removed by another rollback happening at the "
                            "same time." % doc
                        )

                LOG.debug("OplogThread: Rollback, removed %d docs." %
                          remov_inc)

                # Insert the ones from mongo
                LOG.debug("OplogThread: Rollback, inserting documents "
                          "from mongo.")
                insert_inc = 0
                fail_insert_inc = 0
                for doc in to_index:
                    try:
                        insert_inc += 1
                        dm.upsert(doc,
                                  self.dest_mapping_stru.get(namespace,
                                                             namespace),
                                  util.bson_ts_to_long(rollback_cutoff_ts))
                    except errors.OperationFailed:
                        fail_insert_inc += 1
                        LOG.exception("OplogThread: Rollback, Unable to "
                                      "insert %r" % doc)

        LOG.debug("OplogThread: Rollback, Successfully inserted %d "
                  " documents and failed to insert %d"
                  " documents.  Returning a rollback cutoff time of %s "
                  % (insert_inc, fail_insert_inc, str(rollback_cutoff_ts)))

        return rollback_cutoff_ts
