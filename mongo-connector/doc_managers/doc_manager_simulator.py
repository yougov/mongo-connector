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

"""A class to serve as proxy for the target engine for testing.

Receives documents from the oplog worker threads and indexes them
into the backend.

Please look at the Solr and ElasticSearch doc manager classes for a sample
implementation with real systems.
"""


class DocManager():
    """BackendSimulator emulates both a target DocManager and a server.

    The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url=None, unique_key='_id'):
        """Creates a dictionary to hold document id keys mapped to the
        documents as values.
        """
        self.unique_key = unique_key
        self.doc_dict = {}

    def stop(self):
        """Stops any running threads in the DocManager.
        """
        pass

    def upsert(self, doc):
        """Adds a document to the doc dict.
        """

        self.doc_dict[doc[self.unique_key]] = doc

    def remove(self, doc):
        """Removes the document from the doc dict.
        """
        del self.doc_dict[doc[self.unique_key]]

    def search(self, start_ts, end_ts):
        """Searches through all documents and finds all documents within the
        range.

        Since we have very few documents in the doc dict when this is called,
        linear search is fine. This method is only used by rollbacks to query
        all the documents in the target engine within a certain timestamp
        window. The input will be two longs (converted from Bson timestamp)
        which specify the time range. The start_ts refers to the timestamp
        of the last oplog entry after a rollback. The end_ts is the timestamp
        of the last document committed to the backend.
        """
        ret_list = []
        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if ts <= end_ts or ts >= start_ts:
                ret_list.append(stored_doc)

        return ret_list

    def commit(self):
        """Simply passes since we're not using an engine that needs commiting.
        """
        pass

    def get_last_doc(self):
        """Searches through the doc dict to find the document with the latest
            timestamp.
        """

        last_doc = None
        last_ts = None

        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if last_ts is None or ts >= last_ts:
                last_doc = stored_doc
                last_ts = ts

        return last_doc

    def _search(self):
        """Returns all documents in the doc dict.

        This function is not a part of the DocManager API, and is only used
        to simulate searching all documents from a backend.
        """

        ret_list = []
        for doc in self.doc_dict.values():
            ret_list.append(doc)

        return ret_list

    def _delete(self):
        """Deletes all documents.

        This function is not a part of the DocManager API, and is only used
        to simulate deleting all documents from a backend.
        """
        self.doc_dict = {}
