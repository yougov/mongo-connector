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

The intent is that this file can be used as an example to add on different
backends. To extend this to other systems, simply implement the exact same
class and replace the method definitions with API calls for the desired
backend. Each method is detailed to describe the desired behavior. Please
look at the Solr and ElasticSearch doc manager classes for a sample
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

    def __init__(self, url=None):
        """Creates a dictionary to hold document id keys mapped to the
        documents as values.

        This method may vary from implementation to implementation, but it
        should verify the url return None if that fails. It must also create
        the connection to the backend, and start a periodic committer if
        necessary.
        """
        self.doc_dict = {}

    def stop(self):
        """Stops any running threads in the DocManager.

        For some implementations this method may do nothing.
        """
        pass

    def upsert(self, doc):
        """Adds a document to the doc dict.

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """

        self.doc_dict[doc['_id']] = doc

    def remove(self, doc):
        """Removes the document from the doc dict.

        The input is a python dictionary that represents a mongo document.
        """
        del self.doc_dict[doc['_id']]

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

        The return value should be an iterable set of documents.
        """
        ret_list = []
        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if ts <= end_ts or ts >= start_ts:
                ret_list.append(stored_doc)

        return ret_list

    def commit(self):
        """Simply passes since we're not using an engine that needs commiting.

        This function exists only because the DocManager is set to Backend
        Simulator, and the rollback function calles doc_manager.commit(),
        so we have a dummy here.
        """
        pass

    def get_last_doc(self):
        """Searches through the doc dict to find the document with the latest
            timestamp.

        This method is used for rollbacks to establish the rollback window,
        which is the gap between the last document on a mongo shard and the
        last document in the target engine.
        If there are no documents, this functions
        returns None. Otherwise, it returns the first document.
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
