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

from bson.codec_options import CodecOptions, DatetimeConversion

from mongo_connector import errors, util
from mongo_connector.util import hello_or_ismaster
from mongo_connector.constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_BULK_APPLY_IDLE_SECONDS,
    DEFAULT_BULK_APPLY_LAG_SECONDS,
    DEFAULT_MAX_BULK,
)
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector.util import log_fatal_exceptions, retry_until_ok

LOG = logging.getLogger(__name__)

# How often (in processed oplog entries) to refresh the cached timestamp of
# the newest oplog entry when deciding whether we are in catch-up mode.
BULK_LAG_REFRESH_ENTRIES = 500


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
            # The tailable oplog cursor blocks awaiting new data at the
            # tail, so a separate thread is needed to flush an under-full
            # catch-up buffer on an idle source.
            self.opman._maybe_flush_idle_buffer()
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

        # Lag (in seconds behind the newest oplog entry) at which oplog
        # application switches from serial per-entry handling to buffered
        # bulk application via DocManager.bulk_apply. 0 disables it.
        self.bulk_apply_lag_seconds = (
            kwargs.get(
                "bulk_apply_lag_seconds", DEFAULT_BULK_APPLY_LAG_SECONDS
            )
            or 0
        )

        # Number of oplog entries to buffer before flushing them in bulk.
        self.bulk_apply_batch_size = kwargs.get(
            "bulk_apply_batch_size", DEFAULT_MAX_BULK
        )

        # Maximum wall-clock seconds buffered ops may sit unflushed when
        # the batch doesn't fill and no further entries arrive. 0 disables.
        self.bulk_apply_idle_seconds = (
            kwargs.get(
                "bulk_apply_idle_seconds", DEFAULT_BULK_APPLY_IDLE_SECONDS
            )
            or 0
        )

        # Normalized ops buffered for bulk application while catching up.
        # Keyed by (namespace, doc _id) so that only the last op per
        # document in a flush window is applied; the buffer flushes once it
        # holds bulk_apply_batch_size deduplicated documents.
        self._catchup_ops = {}
        # Timestamp of the last buffered entry; checkpointed after a flush.
        self._catchup_last_ts = None
        # Cached newest oplog timestamp used for lag decisions.
        self._newest_oplog_ts = None
        self._entries_since_lag_check = 0
        # Wall-clock time the buffer went from empty to non-empty, used for
        # the idle flush.
        self._catchup_first_buffered_at = None
        # Guards the catch-up buffer and ordered application of ops: the
        # idle flush can be triggered by the ReplicationLagLogger thread.
        self._catchup_lock = threading.RLock()

        LOG.info("OplogThread: Initializing oplog thread")

        # Apply DATETIME_CLAMP to the oplog tailing cursor so out-of-range BSON
        # dates (e.g. year 0) embedded in oplog entries decode to datetime.min
        # instead of raising InvalidBSON and killing the sync thread. The
        # _codec_options class attribute is defined further below; class
        # attributes are accessible here regardless of source order.
        self.oplog = self.primary_client.local.oplog.rs.with_options(
            codec_options=self._codec_options
        )
        self.replset_name = hello_or_ismaster(self.primary_client)["setName"]

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

    def _expand_apply_ops(self, entry):
        """Expand applyOps command entries into their CRUD sub-entries.

        Multi-document writes issued as a single command (insert_many,
        update_many, delete_many) are recorded as a single applyOps entry
        on MongoDB >= 5.x. If every sub-operation is a plain CRUD op, yield
        each sub-op as its own entry so it can be processed like a normal
        oplog entry, carrying the parent entry's timestamp. Otherwise yield
        the original command entry unchanged for serial command handling.
        """
        o = entry.get("o")
        if entry["op"] == "c" and isinstance(o, dict) and "applyOps" in o:
            sub_ops = o["applyOps"]
            if all(s.get("op") in ("i", "u", "d") for s in sub_ops):
                for sub in sub_ops:
                    yield {
                        "op": sub["op"],
                        "ns": sub["ns"],
                        "o": sub.get("o", {}),
                        "o2": sub.get("o2", {}),
                        "ts": entry["ts"],
                    }
                return
        yield entry

    def _bulk_catchup_enabled(self):
        return self.bulk_apply_lag_seconds > 0

    def _is_behind_oplog(self, entry):
        """Determine if an oplog entry is far enough behind the newest entry
        to warrant bulk catch-up application.

        The newest oplog timestamp is cached and refreshed every
        BULK_LAG_REFRESH_ENTRIES processed entries.
        """
        self._entries_since_lag_check += 1
        if (
            self._newest_oplog_ts is None
            or self._entries_since_lag_check >= BULK_LAG_REFRESH_ENTRIES
        ):
            self._newest_oplog_ts = retry_until_ok(self.get_last_oplog_timestamp)
            self._entries_since_lag_check = 0
        newest = self._newest_oplog_ts
        if newest is None:
            return False
        return (newest.time - entry["ts"].time) > self.bulk_apply_lag_seconds

    def _buffer_oplog_entry(self, entry, ns, timestamp):
        """Buffer a normalized op for this entry, deduplicated per document.

        Buffered ops are ``(op_type, namespace, payload, timestamp)`` tuples
        as consumed by DocManager.bulk_apply, keyed by ``(namespace, _id)``.
        Only the last op for a document within a flush window survives:
        every op carries the document's complete state (or resolves to it at
        flush time), so intermediate ops for the same document are
        redundant. Modifier updates are buffered as "fetch" ops and resolved
        to whole documents at flush time.
        """
        operation = entry["op"]
        if operation == "i":
            doc_id = entry["o"]["_id"]
            op = ("upsert", ns, entry["o"], timestamp)
        elif operation == "u":
            doc_id = entry["o2"]["_id"]
            update_spec = entry["o"]
            if (
                "$set" in update_spec
                or "$unset" in update_spec
                or "diff" in update_spec
            ):
                # Modifier update. MongoDB < 5.0 uses $set/$unset specs,
                # MongoDB >= 5.0 uses "diff" specs. Resolve the current
                # document from the source at flush time.
                op = ("fetch", ns, doc_id, timestamp)
            else:
                # Replacement-style update: 'o' holds the new document.
                doc = dict(update_spec)
                doc.pop("$v", None)
                doc["_id"] = doc_id
                op = ("upsert", ns, doc, timestamp)
        elif operation == "d":
            doc_id = entry["o"]["_id"]
            op = ("remove", ns, doc_id, timestamp)
        else:
            return
        with self._catchup_lock:
            if not self._catchup_ops:
                self._catchup_first_buffered_at = time.time()
            self._catchup_ops[(ns, doc_id)] = op
            self._catchup_last_ts = entry["ts"]

    def _resolve_catchup_fetches(self, ops):
        """Replace "fetch" ops with "upsert"/"remove" ops by retrieving the
        current version of each document from the source with batched $in
        queries. Documents no longer present in the source become removes.
        """
        ids_by_ns = {}
        for op_type, ns, doc_id, _ in ops:
            if op_type == "fetch":
                ids_by_ns.setdefault(ns, set()).add(doc_id)
        if not ids_by_ns:
            return ops

        client = self.mongos_client or self.primary_client
        docs = {}
        for ns, doc_ids in ids_by_ns.items():
            src_ns = self.namespace_config.unmap_namespace(ns) or ns
            database, coll = src_ns.split(".", 1)
            collection = client[database].with_options(
                codec_options=self._codec_options
            )[coll]
            found = retry_until_ok(
                lambda: list(
                    collection.find(
                        {"_id": {"$in": list(doc_ids)}},
                        projection=self.namespace_config.projection(src_ns),
                    )
                )
            )
            for doc in found:
                docs[(ns, doc["_id"])] = doc

        resolved = []
        for op_type, ns, payload, ts in ops:
            if op_type != "fetch":
                resolved.append((op_type, ns, payload, ts))
            elif (ns, payload) in docs:
                resolved.append(("upsert", ns, docs[(ns, payload)], ts))
            else:
                resolved.append(("remove", ns, payload, ts))
        return resolved

    def _apply_ops_serially(self, dm, ops):
        """Fallback path applying normalized ops one at a time."""
        for op_type, ns, payload, ts in ops:
            try:
                if op_type == "upsert":
                    dm.upsert(payload, ns, ts)
                elif op_type == "remove":
                    dm.remove(payload, ns, ts)
            except errors.OperationFailed:
                LOG.exception(
                    "Unable to apply %s op for namespace '%s'" % (op_type, ns)
                )
            except errors.ConnectionFailed:
                LOG.exception(
                    "Connection failed applying %s op for namespace '%s'"
                    % (op_type, ns)
                )

    def _flush_catchup_ops(self):
        """Flush buffered catch-up ops to each doc manager and checkpoint.

        If a doc manager's bulk_apply fails, that doc manager falls back to
        applying the buffered ops serially.
        """
        with self._catchup_lock:
            if not self._catchup_ops:
                return
            ops = self._resolve_catchup_fetches(
                list(self._catchup_ops.values())
            )
            for dm in self.doc_managers:
                bulk_apply = getattr(dm, "bulk_apply", None)
                if bulk_apply is not None:
                    try:
                        bulk_apply(ops)
                        continue
                    except Exception:
                        LOG.exception(
                            "Error during bulk apply for doc manager %r, "
                            "replaying buffered ops serially" % (dm,)
                        )
                self._apply_ops_serially(dm, ops)
            self._catchup_ops = {}
            self._catchup_first_buffered_at = None
            if self._catchup_last_ts is not None:
                self.update_checkpoint(self._catchup_last_ts)
                self._catchup_last_ts = None

    def _maybe_flush_idle_buffer(self):
        """Flush the catch-up buffer if it has been pending too long.

        A tailable cursor blocks awaiting new data at the tail, so when the
        backlog is smaller than bulk_apply_batch_size buffered ops would
        otherwise sit unapplied until the next relevant entry arrives. This
        check runs for every entry read (including skipped/no-op entries,
        which arrive periodically on an idle replica set), so an under-full
        buffer flushes within roughly bulk_apply_idle_seconds plus the
        source's heartbeat interval.
        """
        with self._catchup_lock:
            try:
                if (
                    self._catchup_ops
                    and self.bulk_apply_idle_seconds > 0
                    and self._catchup_first_buffered_at is not None
                    and (
                        time.time() - self._catchup_first_buffered_at
                        >= self.bulk_apply_idle_seconds
                    )
                ):
                    LOG.debug(
                        "OplogThread: flushing %d buffered ops after "
                        "idle timeout",
                        len(self._catchup_ops),
                    )
                    self._flush_catchup_ops()
            except Exception:
                LOG.exception("Error flushing idle catch-up buffer")

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
            bulk_inc = 0
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

                        for entry in self._expand_apply_ops(entry):
                            self._maybe_flush_idle_buffer()

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

                            # Commands and GridFS file writes must be applied
                            # serially in oplog order; other operations may be
                            # buffered and applied in bulk while behind.
                            if (
                                self._bulk_catchup_enabled()
                                and not is_gridfs_file
                                and operation != "c"
                                and self._is_behind_oplog(entry)
                            ):
                                self._buffer_oplog_entry(entry, ns, timestamp)
                                bulk_inc += 1
                                if (
                                    len(self._catchup_ops)
                                    >= self.bulk_apply_batch_size
                                ):
                                    self._flush_catchup_ops()
                                continue

                            # Flush buffered ops before serially applying this
                            # entry to preserve oplog ordering.
                            if self._catchup_ops:
                                self._flush_catchup_ops()

                            # The lock prevents a timer-triggered flush
                            # from interleaving with serial application
                            # of a newer entry.
                            with self._catchup_lock:
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

                    # flush any remaining buffered ops before recording the
                    # checkpoint; flushing updates the checkpoint itself.
                    if self._catchup_ops:
                        self._flush_catchup_ops()

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
                # Discard unflushed buffered ops: the checkpoint has not
                # advanced past them, so they will be read again after
                # reconnecting.
                with self._catchup_lock:
                    self._catchup_ops = {}
                    self._catchup_last_ts = None
                    self._catchup_first_buffered_at = None

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
                "upserted: %d, updated: %d, bulk applied: %d"
                % (remove_inc, upsert_inc, update_inc, bulk_inc)
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
                query, cursor_type=CursorType.TAILABLE_AWAIT
            )
        return cursor

    # Codec options for handling out-of-range BSON dates gracefully.
    _codec_options = CodecOptions(datetime_conversion=DatetimeConversion.DATETIME_CLAMP)

    def get_collection(self, namespace):
        """Get a pymongo collection from a namespace.

        Uses DATETIME_CLAMP codec options to handle out-of-range dates
        (e.g., year 0001) by clamping to datetime.min/datetime.max instead
        of raising exceptions.
        """
        database, coll = namespace.split(".", 1)
        db = self.primary_client[database].with_options(
            codec_options=self._codec_options
        )
        return db[coll]

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
                db_list = retry_until_ok(self.primary_client.list_database_names)
            for database in db_list:
                if database == "config" or database == "local":
                    continue
                coll_list = retry_until_ok(
                    self.primary_client[database].list_collection_names
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
                total_docs = retry_until_ok(from_coll.estimated_document_count)
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
                    total_docs = retry_until_ok(from_coll.estimated_document_count)
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
