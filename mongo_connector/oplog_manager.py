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

import pymongo

from pymongo import CursorType

from mongo_connector import errors, util
from mongo_connector.constants import DEFAULT_BATCH_SIZE
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector.util import log_fatal_exceptions, retry_until_ok

LOG = logging.getLogger(__name__)


class ReplicationLagLogger(threading.Thread):
    """Thread that periodically logs the current replication lag.
    """

    def __init__(self, opman, interval):
        super(ReplicationLagLogger, self).__init__()
        self.opman = opman
        self.interval = interval
        self.daemon = True

    def log_replication_lag(self):
        checkpoint = self.opman.checkpoint
        if checkpoint is None:
            return
        newest_write = retry_until_ok(self.opman.get_last_oplog_timestamp)
        if newest_write < checkpoint:
            # OplogThread will perform a rollback, don't log anything
            return
        lag_secs = newest_write.time - checkpoint.time
        if lag_secs > 0:
            LOG.info(
                "OplogThread for replica set '%s' is %s seconds behind " "the oplog.",
                self.opman.replset_name,
                lag_secs,
            )
        else:
            lag_inc = newest_write.inc - checkpoint.inc
            if lag_inc > 0:
                LOG.info(
                    "OplogThread for replica set '%s' is %s entries "
                    "behind the oplog.",
                    self.opman.replset_name,
                    lag_inc,
                )
            else:
                LOG.info(
                    "OplogThread for replica set '%s' is up to date " "with the oplog.",
                    self.opman.replset_name,
                )

    def run(self):
        while self.opman.is_alive():
            self.log_replication_lag()
            time.sleep(self.interval)


class OplogThread(threading.Thread):
    """Thread that tails an oplog.

    Calls the appropriate method on DocManagers for each relevant oplog entry.
    """

    def __init__(
        self,
        primary_client,
        doc_managers,
        oplog_progress_dict,
        namespace_config,
        mongos_client=None,
        **kwargs
    ):
        super(OplogThread, self).__init__()

        self.batch_size = kwargs.get("batch_size", DEFAULT_BATCH_SIZE)

        # The connection to the primary for this replicaSet.
        self.primary_client = primary_client

        # The connection to the mongos, if there is one.
        self.mongos_client = mongos_client

        # Are we allowed to perform a collection dump?
        self.collection_dump = kwargs.get("collection_dump", True)

        self.only_dump = kwargs.get('only_dump', False)

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

        # The namespace configuration
        self.namespace_config = namespace_config

        # Whether the collection dump gracefully handles exceptions
        self.continue_on_error = kwargs.get("continue_on_error", False)

        LOG.info("OplogThread: Initializing oplog thread")

        self.oplog = self.primary_client.local.oplog.rs
        self.replset_name = self.primary_client.admin.command("ismaster")["setName"]

        if not self.oplog.find_one():
            err_msg = "OplogThread: No oplog for thread:"
            LOG.warning("%s %s" % (err_msg, self.primary_client))

    def _should_skip_entry(self, entry):
        """Determine if this oplog entry should be skipped.

        This has the possible side effect of modifying the entry's namespace
        and filtering fields from updates and inserts.
        """
        # Don't replicate entries resulting from chunk moves
        if entry.get("fromMigrate"):
            return True, False

        # Ignore no-ops
        if entry["op"] == "n":
            return True, False
        ns = entry["ns"]

        if "." not in ns:
            return True, False
        coll = ns.split(".", 1)[1]

        # Ignore system collections
        if coll.startswith("system."):
            return True, False

        # Ignore GridFS chunks
        if coll.endswith(".chunks"):
            return True, False

        is_gridfs_file = False
        if coll.endswith(".files"):
            ns = ns[: -len(".files")]
            if self.namespace_config.gridfs_namespace(ns):
                is_gridfs_file = True
            else:
                return True, False

        # Commands should not be ignored, filtered, or renamed. Renaming is
        # handled by the DocManagers via the CommandHelper class.
        if coll == "$cmd":
            return False, False

        # Rename or filter out namespaces that are ignored keeping
        # included gridfs namespaces.
        namespace = self.namespace_config.lookup(ns)
        if namespace is None:
            LOG.debug(
                "OplogThread: Skipping oplog entry: "
                "'%s' is not in the namespace configuration." % (ns,)
            )
            return True, False

        # Update the namespace.
        entry["ns"] = namespace.dest_name

        # Take fields out of the oplog entry that shouldn't be replicated.
        # This may nullify the document if there's nothing to do.
        if not self.filter_oplog_entry(
            entry,
            include_fields=namespace.include_fields,
            exclude_fields=namespace.exclude_fields,
        ):
            return True, False
        return False, is_gridfs_file

    @log_fatal_exceptions
    def run(self):
        """Start the oplog worker.
        """
        ReplicationLagLogger(self, 30).start()
        LOG.debug("OplogThread: Run thread started")
        while self.running is True:
            LOG.debug("OplogThread: Getting cursor")
            cursor, cursor_empty = retry_until_ok(self.init_cursor)
            # we've fallen too far behind
            if cursor is None and self.checkpoint is not None:
                err_msg = "OplogThread: Last entry no longer in oplog"
                effect = "cannot recover!"
                LOG.error("%s %s %s" % (err_msg, effect, self.oplog))
                self.running = False
                continue

            if cursor_empty:
                LOG.debug(
                    "OplogThread: Last entry is the one we "
                    "already processed.  Up to date.  Sleeping."
                )
                time.sleep(1)
                continue

            last_ts = None
            remove_inc = 0
            upsert_inc = 0
            update_inc = 0
            try:
                LOG.debug("OplogThread: about to process new oplog entries")
                while cursor.alive and self.running:
                    LOG.debug(
                        "OplogThread: Cursor is still"
                        " alive and thread is still running."
                    )
                    for n, entry in enumerate(cursor):
                        # Break out if this thread should stop
                        if not self.running:
                            break

                        LOG.debug(
                            "OplogThread: Iterating through cursor,"
                            " document number in this cursor is %d" % n
                        )

                        skip, is_gridfs_file = self._should_skip_entry(entry)
                        if skip:
                            # update the last_ts on skipped entries to ensure
                            # our checkpoint does not fall off the oplog. This
                            # also prevents reprocessing skipped entries.
                            last_ts = entry["ts"]
                            continue

                        # Sync the current oplog operation
                        operation = entry["op"]
                        ns = entry["ns"]
                        timestamp = util.bson_ts_to_long(entry["ts"])
                        for docman in self.doc_managers:
                            try:
                                LOG.debug(
                                    "OplogThread: Operation for this "
                                    "entry is %s" % str(operation)
                                )

                                # Remove
                                if operation == "d":
                                    docman.remove(entry["o"]["_id"], ns, timestamp)
                                    remove_inc += 1

                                # Insert
                                elif operation == "i":  # Insert
                                    # Retrieve inserted document from
                                    # 'o' field in oplog record
                                    doc = entry.get("o")
                                    # Extract timestamp and namespace
                                    if is_gridfs_file:
                                        db, coll = ns.split(".", 1)
                                        gridfile = GridFSFile(
                                            self.primary_client[db][coll], doc
                                        )
                                        docman.insert_file(gridfile, ns, timestamp)
                                    else:
                                        docman.upsert(doc, ns, timestamp)
                                    upsert_inc += 1

                                # Update
                                elif operation == "u":
                                    docman.update(
                                        entry["o2"]["_id"], entry["o"], ns, timestamp
                                    )
                                    update_inc += 1

                                # Command
                                elif operation == "c":
                                    # use unmapped namespace
                                    doc = entry.get("o")
                                    docman.handle_command(doc, entry["ns"], timestamp)

                            except errors.OperationFailed:
                                LOG.exception(
                                    "Unable to process oplog document %r" % entry
                                )
                            except errors.ConnectionFailed:
                                LOG.exception(
                                    "Connection failed while processing oplog "
                                    "document %r" % entry
                                )

                        if (remove_inc + upsert_inc + update_inc) % 1000 == 0:
                            LOG.debug(
                                "OplogThread: Documents removed: %d, "
                                "inserted: %d, updated: %d so far"
                                % (remove_inc, upsert_inc, update_inc)
                            )

                        LOG.debug("OplogThread: Doc is processed.")

                        last_ts = entry["ts"]

                        # update timestamp per batch size
                        # n % -1 (default for self.batch_size) == 0 for all n
                        if n % self.batch_size == 1:
                            self.update_checkpoint(last_ts)
                            last_ts = None

                    # update timestamp after running through oplog
                    if last_ts is not None:
                        LOG.debug(
                            "OplogThread: updating checkpoint after "
                            "processing new oplog entries"
                        )
                        self.update_checkpoint(last_ts)

            except (
                pymongo.errors.AutoReconnect,
                pymongo.errors.OperationFailure,
                pymongo.errors.ConfigurationError,
            ):
                LOG.exception(
                    "Cursor closed due to an exception. " "Will attempt to reconnect."
                )

            # update timestamp before attempting to reconnect to MongoDB,
            # after being join()'ed, or if the cursor closes
            if last_ts is not None:
                LOG.debug(
                    "OplogThread: updating checkpoint after an "
                    "Exception, cursor closing, or join() on this"
                    "thread."
                )
                self.update_checkpoint(last_ts)

            LOG.debug(
                "OplogThread: Sleeping. Documents removed: %d, "
                "upserted: %d, updated: %d" % (remove_inc, upsert_inc, update_inc)
            )
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
        path = field.split(".")
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
                    if key.startswith(field) and key[len(field)] == ".":
                        yield [key], doc[key]
                        # Continue searching, there may be multiple matches.
                        # For example, field 'a' should match 'a.b' and 'a.c'.
                elif len(key) < len(field):
                    # Handle case where key is a prefix of field, eg field is
                    # 'a.b' and key is 'a'.
                    if field.startswith(key) and field[len(key)] == ".":
                        # Search for the remaining part of the field
                        matched = cls._find_field(field[len(key) + 1 :], doc[key])
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

    def _pop_excluded_fields(self, doc, exclude_fields, update=False):
        # Remove all the fields that were passed in exclude_fields.
        find_fields = self._find_update_fields if update else self._find_field
        for field in exclude_fields:
            for path, _ in find_fields(field, doc):
                # Delete each matching field in the original document.
                temp_doc = doc
                for p in path[:-1]:
                    temp_doc = temp_doc[p]
                temp_doc.pop(path[-1])

        return doc  # Need this to be similar to copy_included_fields.

    def _copy_included_fields(self, doc, include_fields, update=False):
        new_doc = {}
        find_fields = self._find_update_fields if update else self._find_field
        for field in include_fields:
            for path, value in find_fields(field, doc):
                # Copy each matching field in the original document.
                temp_doc = new_doc
                for p in path[:-1]:
                    temp_doc = temp_doc.setdefault(p, {})
                temp_doc[path[-1]] = value

        return new_doc

    def filter_oplog_entry(self, entry, include_fields=None, exclude_fields=None):
        """Remove fields from an oplog entry that should not be replicated.

        NOTE: this does not support array indexing, for example 'a.b.2'"""
        if not include_fields and not exclude_fields:
            return entry
        elif include_fields:
            filter_fields = self._copy_included_fields
        else:
            filter_fields = self._pop_excluded_fields

        fields = include_fields or exclude_fields
        entry_o = entry["o"]
        # Version 3.6 of mongodb includes a $v,
        # see https://jira.mongodb.org/browse/SERVER-32240
        if "$v" in entry_o:
            entry_o.pop("$v")

        # 'i' indicates an insert. 'o' field is the doc to be inserted.
        if entry["op"] == "i":
            entry["o"] = filter_fields(entry_o, fields)
        # 'u' indicates an update. The 'o' field describes an update spec
        # if '$set' or '$unset' are present.
        elif entry["op"] == "u" and ("$set" in entry_o or "$unset" in entry_o):
            if "$set" in entry_o:
                entry["o"]["$set"] = filter_fields(entry_o["$set"], fields, update=True)
            if "$unset" in entry_o:
                entry["o"]["$unset"] = filter_fields(
                    entry_o["$unset"], fields, update=True
                )
            # not allowed to have empty $set/$unset, so remove if empty
            if "$set" in entry_o and not entry_o["$set"]:
                entry_o.pop("$set")
            if "$unset" in entry_o and not entry_o["$unset"]:
                entry_o.pop("$unset")
            if not entry_o:
                return None
        # 'u' indicates an update. The 'o' field is the replacement document
        # if no '$set' or '$unset' are present.
        elif entry["op"] == "u":
            entry["o"] = filter_fields(entry_o, fields)

        return entry

    def get_oplog_cursor(self, timestamp=None):
        """Get a cursor to the oplog after the given timestamp, excluding
        no-op entries.

        If no timestamp is specified, returns a cursor to the entire oplog.
        """
        query = {"op": {"$ne": "n"}}
        if timestamp is None:
            cursor = self.oplog.find(query, cursor_type=CursorType.TAILABLE_AWAIT)
        else:
            query["ts"] = {"$gte": timestamp}
            cursor = self.oplog.find(
                query, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True
            )
        return cursor

    def get_collection(self, namespace):
        """Get a pymongo collection from a namespace."""
        database, coll = namespace.split(".", 1)
        return self.primary_client[database][coll]

    def dump_collection(self):
        """Dumps collection into the target system.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """

        timestamp = retry_until_ok(self.get_last_oplog_timestamp)
        if timestamp is None:
            return None
        long_ts = util.bson_ts_to_long(timestamp)
        # Flag if this oplog thread was cancelled during the collection dump.
        # Use a list to workaround python scoping.
        dump_cancelled = [False]

        def get_all_ns():
            ns_set = []
            gridfs_ns_set = []
            db_list = self.namespace_config.get_included_databases()
            if not db_list:
                # Only use listDatabases when the configured databases are not
                # explicit.
                db_list = retry_until_ok(self.primary_client.database_names)
            for database in db_list:
                if database == "config" or database == "local":
                    continue
                coll_list = retry_until_ok(
                    self.primary_client[database].collection_names
                )
                for coll in coll_list:
                    # ignore system collections
                    if coll.startswith("system."):
                        continue
                    # ignore gridfs chunks collections
                    if coll.endswith(".chunks"):
                        continue
                    if coll.endswith(".files"):
                        namespace = "%s.%s" % (database, coll)
                        namespace = namespace[: -len(".files")]
                        if self.namespace_config.gridfs_namespace(namespace):
                            gridfs_ns_set.append(namespace)
                    else:
                        namespace = "%s.%s" % (database, coll)
                        if self.namespace_config.map_namespace(namespace):
                            ns_set.append(namespace)
            return ns_set, gridfs_ns_set

        dump_set, gridfs_dump_set = get_all_ns()

        LOG.debug("OplogThread: Dumping set of collections %s " % dump_set)

        def docs_to_dump(from_coll):
            last_id = None
            attempts = 0
            projection = self.namespace_config.projection(from_coll.full_name)
            # Loop to handle possible AutoReconnect
            while attempts < 60:
                if last_id is None:
                    cursor = retry_until_ok(
                        from_coll.find,
                        projection=projection,
                        sort=[("_id", pymongo.ASCENDING)],
                    )
                else:
                    cursor = retry_until_ok(
                        from_coll.find,
                        {"_id": {"$gt": last_id}},
                        projection=projection,
                        sort=[("_id", pymongo.ASCENDING)],
                    )
                try:
                    for doc in cursor:
                        if not self.running:
                            # Thread was joined while performing the
                            # collection dump.
                            dump_cancelled[0] = True
                            raise StopIteration
                        last_id = doc["_id"]
                        yield doc
                    break
                except (pymongo.errors.AutoReconnect, pymongo.errors.OperationFailure):
                    attempts += 1
                    time.sleep(1)

        def upsert_each(dm):
            num_failed = 0
            for namespace in dump_set:
                from_coll = self.get_collection(namespace)
                mapped_ns = self.namespace_config.map_namespace(namespace)
                total_docs = retry_until_ok(from_coll.count)
                num = None
                for num, doc in enumerate(docs_to_dump(from_coll)):
                    try:
                        dm.upsert(doc, mapped_ns, long_ts)
                    except Exception:
                        if self.continue_on_error:
                            LOG.exception("Could not upsert document: %r" % doc)
                            num_failed += 1
                        else:
                            raise
                    if num % 10000 == 0:
                        LOG.info(
                            "Upserted %d out of approximately %d docs "
                            "from collection '%s'",
                            num + 1,
                            total_docs,
                            namespace,
                        )
                if num is not None:
                    LOG.info(
                        "Upserted %d out of approximately %d docs from "
                        "collection '%s'",
                        num + 1,
                        total_docs,
                        namespace,
                    )
            if num_failed > 0:
                LOG.error("Failed to upsert %d docs" % num_failed)

        def upsert_all(dm):
            try:
                for namespace in dump_set:
                    from_coll = self.get_collection(namespace)
                    total_docs = retry_until_ok(from_coll.count)
                    mapped_ns = self.namespace_config.map_namespace(namespace)
                    LOG.info(
                        "Bulk upserting approximately %d docs from " "collection '%s'",
                        total_docs,
                        namespace,
                    )
                    dm.bulk_upsert(docs_to_dump(from_coll), mapped_ns, long_ts)
            except Exception:
                if self.continue_on_error:
                    LOG.exception(
                        "OplogThread: caught exception"
                        " during bulk upsert, re-upserting"
                        " documents serially"
                    )
                    upsert_each(dm)
                else:
                    raise

        def do_dump(dm, error_queue):
            try:
                LOG.debug(
                    "OplogThread: Using bulk upsert function for " "collection dump"
                )
                upsert_all(dm)

                if gridfs_dump_set:
                    LOG.info(
                        "OplogThread: dumping GridFS collections: %s", gridfs_dump_set
                    )

                # Dump GridFS files
                for gridfs_ns in gridfs_dump_set:
                    mongo_coll = self.get_collection(gridfs_ns)
                    from_coll = self.get_collection(gridfs_ns + ".files")
                    dest_ns = self.namespace_config.map_namespace(gridfs_ns)
                    for doc in docs_to_dump(from_coll):
                        gridfile = GridFSFile(mongo_coll, doc)
                        dm.insert_file(gridfile, dest_ns, long_ts)
            except Exception:
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
                LOG.critical(
                    "Exception during collection dump", exc_info=errors.get_nowait()
                )
                dump_success = False
        except queue.Empty:
            pass

        if not dump_success:
            err_msg = "OplogThread: Failed during dump collection"
            effect = "cannot recover!"
            LOG.error("%s %s %s" % (err_msg, effect, self.oplog))
            self.running = False
            return None

        if dump_cancelled[0]:
            LOG.warning(
                "Initial collection dump was interrupted. "
                "Will re-run the collection dump on next startup."
            )
            return None

        return timestamp

    def _get_oplog_timestamp(self, newest_entry):
        """Return the timestamp of the latest or earliest entry in the oplog.
        """
        sort_order = pymongo.DESCENDING if newest_entry else pymongo.ASCENDING
        curr = (
            self.oplog.find({"op": {"$ne": "n"}}).sort("$natural", sort_order).limit(-1)
        )

        try:
            ts = next(curr)["ts"]
        except StopIteration:
            LOG.debug("OplogThread: oplog is empty.")
            return None

        LOG.debug(
            "OplogThread: %s oplog entry has timestamp %s."
            % ("Newest" if newest_entry else "Oldest", ts)
        )
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
            # Tailable cursors can not have singleBatch=True in MongoDB > 3.3
            next(cursor.clone().remove_option(CursorType.TAILABLE_AWAIT).limit(-1))
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

        if timestamp is None or self.only_dump:
            if self.collection_dump:
                # dump collection and update checkpoint
                timestamp = self.dump_collection()
                if self.only_dump:
                    LOG.info("Finished dump. Exiting.")
                    timestamp = None
                    self.running = False

                self.update_checkpoint(timestamp)
                if timestamp is None:
                    return None, True
            else:
                # Collection dump disabled:
                # Return cursor to beginning of oplog but do not set the
                # checkpoint. The checkpoint will be set after an operation
                # has been applied.
                cursor = self.get_oplog_cursor()
                return cursor, self._cursor_empty(cursor)

        cursor = self.get_oplog_cursor(timestamp)
        cursor_empty = self._cursor_empty(cursor)

        if cursor_empty:
            # rollback, update checkpoint, and retry
            LOG.debug("OplogThread: Initiating rollback from " "get_oplog_cursor")
            self.update_checkpoint(self.rollback())
            return self.init_cursor()

        first_oplog_entry = next(cursor)

        oldest_ts_long = util.bson_ts_to_long(self.get_oldest_oplog_timestamp())
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
            LOG.debug(
                "OplogThread: Initiating rollback from "
                "get_oplog_cursor: new oplog entries found but "
                "checkpoint is not present"
            )
            self.update_checkpoint(self.rollback())
            return self.init_cursor()

        # first entry has been consumed
        return cursor, cursor_empty

    def update_checkpoint(self, checkpoint):
        """Store the current checkpoint in the oplog progress dictionary.
        """
        if checkpoint is not None and checkpoint != self.checkpoint:
            self.checkpoint = checkpoint
            with self.oplog_progress as oplog_prog:
                oplog_dict = oplog_prog.get_dict()
                # If we have the repr of our oplog collection
                # in the dictionary, remove it and replace it
                # with our replica set name.
                # This allows an easy upgrade path from mongo-connector 2.3.
                # For an explanation of the format change, see the comment in
                # read_last_checkpoint.
                oplog_dict.pop(str(self.oplog), None)
                oplog_dict[self.replset_name] = checkpoint
                LOG.debug("OplogThread: oplog checkpoint updated to %s", checkpoint)
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

        LOG.debug("OplogThread: reading last checkpoint as %s " % str(ret_val))
        self.checkpoint = ret_val
        return ret_val

    def rollback(self):
        """Rollback target system to consistent state.

        The strategy is to find the latest timestamp in the target system and
        the largest timestamp in the oplog less than the latest target system
        timestamp. This defines the rollback window and we just roll these
        back until the oplog and target system are in consistent states.
        """
        # Find the most recently inserted document in each target system
        LOG.debug(
            "OplogThread: Initiating rollback sequence to bring "
            "system into a consistent state."
        )
        last_docs = []
        for dm in self.doc_managers:
            dm.commit()
            last_docs.append(dm.get_last_doc())

        # Of these documents, which is the most recent?
        last_inserted_doc = max(
            last_docs, key=lambda x: x["_ts"] if x else float("-inf")
        )

        # Nothing has been replicated. No need to rollback target systems
        if last_inserted_doc is None:
            return None

        # Find the oplog entry that touched the most recent document.
        # We'll use this to figure where to pick up the oplog later.
        target_ts = util.long_to_bson_ts(last_inserted_doc["_ts"])
        last_oplog_entry = util.retry_until_ok(
            self.oplog.find_one,
            {"ts": {"$lte": target_ts}, "op": {"$ne": "n"}},
            sort=[("$natural", pymongo.DESCENDING)],
        )

        LOG.debug("OplogThread: last oplog entry is %s" % str(last_oplog_entry))

        # The oplog entry for the most recent document doesn't exist anymore.
        # If we've fallen behind in the oplog, this will be caught later
        if last_oplog_entry is None:
            return None

        # rollback_cutoff_ts happened *before* the rollback
        rollback_cutoff_ts = last_oplog_entry["ts"]
        start_ts = util.bson_ts_to_long(rollback_cutoff_ts)
        # timestamp of the most recent document on any target system
        end_ts = last_inserted_doc["_ts"]

        for dm in self.doc_managers:
            rollback_set = {}  # this is a dictionary of ns:list of docs

            # group potentially conflicted documents by namespace
            for doc in dm.search(start_ts, end_ts):
                if doc["ns"] in rollback_set:
                    rollback_set[doc["ns"]].append(doc)
                else:
                    rollback_set[doc["ns"]] = [doc]

            # retrieve these documents from MongoDB, either updating
            # or removing them in each target system
            for namespace, doc_list in rollback_set.items():
                # Get the original namespace
                original_namespace = self.namespace_config.unmap_namespace(namespace)
                if not original_namespace:
                    original_namespace = namespace

                database, coll = original_namespace.split(".", 1)
                obj_id = bson.objectid.ObjectId
                bson_obj_id_list = [obj_id(doc["_id"]) for doc in doc_list]

                # Use connection to whole cluster if in sharded environment.
                client = self.mongos_client or self.primary_client
                to_update = util.retry_until_ok(
                    client[database][coll].find,
                    {"_id": {"$in": bson_obj_id_list}},
                    projection=self.namespace_config.projection(original_namespace),
                )
                # Doc list are docs in target system, to_update are
                # Docs in mongo
                doc_hash = {}  # Hash by _id
                for doc in doc_list:
                    doc_hash[bson.objectid.ObjectId(doc["_id"])] = doc

                to_index = []

                def collect_existing_docs():
                    for doc in to_update:
                        if doc["_id"] in doc_hash:
                            del doc_hash[doc["_id"]]
                            to_index.append(doc)

                retry_until_ok(collect_existing_docs)

                # Delete the inconsistent documents
                LOG.debug("OplogThread: Rollback, removing inconsistent " "docs.")
                remov_inc = 0
                for document_id in doc_hash:
                    try:
                        dm.remove(
                            document_id,
                            namespace,
                            util.bson_ts_to_long(rollback_cutoff_ts),
                        )
                        remov_inc += 1
                        LOG.debug("OplogThread: Rollback, removed %r " % doc)
                    except errors.OperationFailed:
                        LOG.warning(
                            "Could not delete document during rollback: %r "
                            "This can happen if this document was already "
                            "removed by another rollback happening at the "
                            "same time." % doc
                        )

                LOG.debug("OplogThread: Rollback, removed %d docs." % remov_inc)

                # Insert the ones from mongo
                LOG.debug("OplogThread: Rollback, inserting documents " "from mongo.")
                insert_inc = 0
                fail_insert_inc = 0
                for doc in to_index:
                    try:
                        insert_inc += 1
                        dm.upsert(
                            doc, namespace, util.bson_ts_to_long(rollback_cutoff_ts)
                        )
                    except errors.OperationFailed:
                        fail_insert_inc += 1
                        LOG.exception(
                            "OplogThread: Rollback, Unable to " "insert %r" % doc
                        )

        LOG.debug(
            "OplogThread: Rollback, Successfully inserted %d "
            " documents and failed to insert %d"
            " documents.  Returning a rollback cutoff time of %s "
            % (insert_inc, fail_insert_inc, str(rollback_cutoff_ts))
        )

        return rollback_cutoff_ts
