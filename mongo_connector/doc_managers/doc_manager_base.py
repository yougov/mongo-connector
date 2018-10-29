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

import logging
import sys

from mongo_connector.connector import get_mininum_mongodb_version
from mongo_connector.errors import UpdateDoesNotApply


LOG = logging.getLogger(__name__)


class DocManagerBase(object):
    """Base class for all DocManager implementations."""

    def apply_update(self, doc, update_spec):
        """Apply an update operation to a document."""

        # Helper to cast a key for a list or dict, or raise ValueError
        def _convert_or_raise(container, key):
            if isinstance(container, dict):
                return key
            elif isinstance(container, list):
                return int(key)
            else:
                raise ValueError

        # Helper to retrieve (and/or create)
        # a dot-separated path within a document.
        def _retrieve_path(container, path, create=False):
            looking_at = container
            for part in path:
                if isinstance(looking_at, dict):
                    if create and part not in looking_at:
                        looking_at[part] = {}
                    looking_at = looking_at[part]
                elif isinstance(looking_at, list):
                    index = int(part)
                    # Do we need to create additional space in the array?
                    if create and len(looking_at) <= index:
                        # Fill buckets with None up to the index we need.
                        looking_at.extend([None] * (index - len(looking_at)))
                        # Bucket we need gets the empty dictionary.
                        looking_at.append({})
                    looking_at = looking_at[index]
                else:
                    raise ValueError
            return looking_at

        def _set_field(doc, to_set, value):
            if "." in to_set:
                path = to_set.split(".")
                where = _retrieve_path(doc, path[:-1], create=True)
                index = _convert_or_raise(where, path[-1])
                wl = len(where)
                if isinstance(where, list) and index >= wl:
                    where.extend([None] * (index + 1 - wl))
                where[index] = value
            else:
                doc[to_set] = value

        def _unset_field(doc, to_unset):
            try:
                if "." in to_unset:
                    path = to_unset.split(".")
                    where = _retrieve_path(doc, path[:-1])
                    index_or_key = _convert_or_raise(where, path[-1])
                    if isinstance(where, list):
                        # Unset an array element sets it to null.
                        where[index_or_key] = None
                    else:
                        # Unset field removes it entirely.
                        del where[index_or_key]
                else:
                    del doc[to_unset]
            except (KeyError, IndexError, ValueError):
                source_version = get_mininum_mongodb_version()
                if source_version is None or source_version.at_least(2, 6):
                    raise
                # Ignore unset errors since MongoDB 2.4 records invalid
                # $unsets in the oplog.
                LOG.warning(
                    "Could not unset field %r from document %r. "
                    "This may be normal when replicating from "
                    "MongoDB 2.4 or the destination could be out of "
                    "sync." % (to_unset, doc)
                )

        # wholesale document replacement
        if "$set" not in update_spec and "$unset" not in update_spec:
            # update spec contains the new document in its entirety
            return update_spec
        else:
            try:
                # $set
                for to_set in update_spec.get("$set", []):
                    value = update_spec["$set"][to_set]
                    _set_field(doc, to_set, value)

                # $unset
                for to_unset in update_spec.get("$unset", []):
                    _unset_field(doc, to_unset)

            except (KeyError, ValueError, AttributeError, IndexError):
                exc_t, exc_v, exc_tb = sys.exc_info()
                msg = "Cannot apply update %r to %r" % (update_spec, doc)
                raise UpdateDoesNotApply(msg).with_traceback(exc_tb)
            return doc

    def bulk_upsert(self, docs, namespace, timestamp):
        """Upsert each document in a set of documents.

        This method may be overridden to upsert many documents at once.
        """
        for doc in docs:
            self.upsert(doc, namespace, timestamp)

    def update(self, doc, update_spec, namespace, timestamp):
        """Update a document.

        ``update_spec`` is the update operation as provided by an oplog record
        in the "o" field.
        """
        raise NotImplementedError

    def upsert(self, document, namespace, timestamp):
        """(Re-)insert a document."""
        raise NotImplementedError

    def remove(self, document_id, namespace, timestamp):
        """Remove a document.

        ``document_id`` is a dict that provides the id of the document
        to be removed. ``namespace`` and ``timestamp`` provide the database +
        collection name and the timestamp from the corresponding oplog entry.
        """
        raise NotImplementedError

    def insert_file(self, f, namespace, timestamp):
        """Insert a file from GridFS."""
        raise NotImplementedError

    def handle_command(self, command_doc, namespace, timestamp):
        """Handle a MongoDB command."""
        raise NotImplementedError

    def search(self, start_ts, end_ts):
        """Get an iterable of documents that were inserted, updated, or deleted
        between ``start_ts`` and ``end_ts``.
        """
        raise NotImplementedError

    def commit(self):
        """Commit all outstanding writes."""
        raise NotImplementedError

    def get_last_doc(self):
        """Get the document that was modified or deleted most recently."""
        raise NotImplementedError

    def stop(self):
        """Stop all threads started by this DocManager."""
        raise NotImplementedError
