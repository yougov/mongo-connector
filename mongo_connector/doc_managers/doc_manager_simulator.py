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

"""A class to serve as proxy for the target engine for testing.

Receives documents from the oplog worker threads and indexes them
into the backend.

Please look at the Solr and ElasticSearch doc manager classes for a sample
implementation with real systems.
"""

from threading import RLock

from mongo_connector import constants
from mongo_connector.errors import OperationFailed
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

__version__ = constants.__version__
"""DocManager Simulator version information

This is packaged with mongo-connector so it shares the same version.
Downstream DocManager implementations should add their package __version__
string here, for example:

__version__ = '0.1.0'
"""


class DocumentStore(dict):
    def __init__(self):
        self._lock = RLock()

    def __getitem__(self, key):
        with self._lock:
            return super(DocumentStore, self).__getitem__(key)

    def __setitem__(self, key, value):
        with self._lock:
            return super(DocumentStore, self).__setitem__(key, value)

    def __iter__(self):
        def __myiter__():
            with self._lock:
                for item in super(DocumentStore, self).__iter__():
                    yield item

        return __myiter__()


class Entry(object):
    def __init__(self, doc, ns, ts):
        self.doc, self.ns, self.ts = doc, ns, ts
        self._id = self.doc["_id"]

    @property
    def meta_dict(self):
        return {"_id": self._id, "ns": self.ns, "_ts": self.ts}

    @property
    def merged_dict(self):
        d = self.doc.copy()
        d.update(**self.meta_dict)
        return d

    def update(self, ns, ts):
        self.ns, self.ts = ns, ts


class DocManager(DocManagerBase):
    """BackendSimulator emulates both a target DocManager and a server.

    The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(
        self,
        url=None,
        unique_key="_id",
        auto_commit_interval=None,
        chunk_size=constants.DEFAULT_MAX_BULK,
        **kwargs
    ):
        """Creates a dictionary to hold document id keys mapped to the
        documents as values.
        """
        self.unique_key = unique_key
        self.auto_commit_interval = auto_commit_interval
        self.doc_dict = DocumentStore()
        self.url = url
        self.chunk_size = chunk_size
        self.kwargs = kwargs

    def stop(self):
        """Stops any running threads in the DocManager.
        """
        pass

    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        document = self.doc_dict[document_id].doc
        updated = self.apply_update(document, update_spec)
        if "_id" in updated:
            updated.pop("_id")
        updated[self.unique_key] = document_id
        self.upsert(updated, namespace, timestamp)
        return updated

    def upsert(self, doc, namespace, timestamp):
        """Adds a document to the doc dict.
        """

        # Allow exceptions to be triggered (for testing purposes)
        if doc.get("_upsert_exception"):
            raise Exception("upsert exception")

        doc_id = doc["_id"]
        self.doc_dict[doc_id] = Entry(doc=doc, ns=namespace, ts=timestamp)

    def insert_file(self, f, namespace, timestamp):
        """Inserts a file to the doc dict.
        """
        doc = f.get_metadata()
        doc["content"] = f.read()
        self.doc_dict[f._id] = Entry(doc=doc, ns=namespace, ts=timestamp)

    def remove(self, document_id, namespace, timestamp):
        """Removes the document from the doc dict.
        """
        try:
            entry = self.doc_dict[document_id]
            entry.doc = None
            entry.update(namespace, timestamp)
        except KeyError:
            raise OperationFailed("Document does not exist: %s" % document_id)

    def search(self, start_ts, end_ts):
        """Searches through all documents and finds all documents that were
        modified or deleted within the range.

        Since we have very few documents in the doc dict when this is called,
        linear search is fine. This method is only used by rollbacks to query
        all the documents in the target engine within a certain timestamp
        window. The input will be two longs (converted from Bson timestamp)
        which specify the time range. The start_ts refers to the timestamp
        of the last oplog entry after a rollback. The end_ts is the timestamp
        of the last document committed to the backend.
        """
        for _id in self.doc_dict:
            entry = self.doc_dict[_id]
            if entry.ts <= end_ts or entry.ts >= start_ts:
                yield entry.meta_dict

    def commit(self):
        """Simply passes since we're not using an engine that needs commiting.
        """
        pass

    def get_last_doc(self):
        """Searches through the doc dict to find the document that was
        modified or deleted most recently."""
        return max(self.doc_dict.values(), key=lambda x: x.ts).meta_dict

    def handle_command(self, command_doc, namespace, timestamp):
        pass

    def _search(self):
        """Returns all documents in the doc dict.

        This function is not a part of the DocManager API, and is only used
        to simulate searching all documents from a backend.
        """
        results = []
        for _id in self.doc_dict:
            entry = self.doc_dict[_id]
            if entry.doc is not None:
                results.append(entry.merged_dict)
        return results

    def _delete(self):
        """Deletes all documents.

        This function is not a part of the DocManager API, and is only used
        to simulate deleting all documents from a backend.
        """
        self.doc_dict = {}
