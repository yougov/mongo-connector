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

"""Receives documents from the oplog worker threads and indexes them
    into the backend.

    This file is a document manager for the Elastic search engine, but the
    intent is that this file can be used as an example to add on different
    backends. To extend this to other systems, simply implement the exact
    same class and replace the method definitions with API calls for the
    desired backend.
    """
import logging
import bson.json_util as bsjson

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from mongo_connector import errors
from threading import Timer
from mongo_connector.util import retry_until_ok

class DocManager():
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using elastic native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, auto_commit=False, unique_key='_id', **kwargs):
        """ Establish a connection to Elastic
        """
        self.elastic = Elasticsearch(hosts=[url])
        self.auto_commit = auto_commit
        self.doc_type = 'string'  # default type is string, change if needed
        self.unique_key = unique_key
        if auto_commit:
            self.run_auto_commit()

    def stop(self):
        """ Stops the instance
        """
        self.auto_commit = False

    def upsert(self, doc):
        """Update or insert a document into Elastic

        If you'd like to have different types of document in your database,
        you can store the doc type as a field in Mongo and set doc_type to
        that field. (e.g. doc_type = doc['_type'])

        """
        doc_type = self.doc_type
        index = doc['ns']
        doc[self.unique_key] = str(doc[self.unique_key])
        doc_id = doc[self.unique_key]
        try:
            self.elastic.index(index=index, doc_type=doc_type,
                               body=bsjson.dumps(doc), id=doc_id, refresh=True)
        except (es_exceptions.ConnectionError):
            raise errors.ConnectionFailed("Could not connect to Elastic Search")
        except es_exceptions.TransportError:
            raise errors.OperationFailed("Could not index document: %s"%(
                bsjson.dumps(doc)))

    def remove(self, doc):
        """Removes documents from Elastic

        The input is a python dictionary that represents a mongo document.
        """
        try:
            self.elastic.delete(index=doc['ns'], doc_type=self.doc_type,
                                id=str(doc[self.unique_key]), refresh=True)
        except (es_exceptions.ConnectionError):
            raise errors.ConnectionFailed("Could not connect to Elastic Search")
        except es_exceptions.TransportError:
            raise errors.OperationFailed("Could not remove document: %s"%(
                bsjson.dumps(doc)))

    def _remove(self):
        """For test purposes only. Removes all documents in test.test
        """
        try:
            self.elastic.delete_by_query(index="test.test",
                                         doc_type=self.doc_type,
                                         body={"match_all":{}})
        except (es_exceptions.ConnectionError):
            raise errors.ConnectionFailed("Could not connect to Elastic Search")
        except es_exceptions.TransportError:
            raise errors.OperationFailed("Could not remove test documents")
        self.commit()

    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results"""
        try:
            first_response = self.elastic.search(*args, search_type="scan",
                                                 scroll="10m", size=100,
                                                 **kwargs)
            scroll_id = first_response.get("_scroll_id")
            expected_count = first_response.get("hits", {}).get("total", 0)
            results_returned = 0
            while results_returned < expected_count:
                next_response = self.elastic.scroll(scroll_id=scroll_id,
                                                    scroll="10m")
                results_returned += len(next_response["hits"]["hits"])
                for doc in next_response["hits"]["hits"]:
                    yield doc["_source"]
        except (es_exceptions.ConnectionError):
            raise errors.ConnectionFailed(
                "Could not connect to Elastic Search")
        except es_exceptions.TransportError:
            raise errors.OperationFailed(
                "Could not retrieve documents from Elastic Search")


    def search(self, start_ts, end_ts):
        """Called to query Elastic for documents in a time range.
        """
        return self._stream_search(index="_all",
                                   body={"query": {"range": {"_ts": {
                                       "gte": start_ts,
                                       "lte": end_ts
                                   }}}})

    def _search(self):
        """For test purposes only. Performs search on Elastic with empty query.
        Does not have to be implemented.
        """
        return self._stream_search(index="test.test",
                                   body={"query": {"match_all": {}}})

    def commit(self):
        """This function is used to force a refresh/commit.
        """
        retry_until_ok(self.elastic.indices.refresh, index="")

    def run_auto_commit(self):
        """Periodically commits to the Elastic server.
        """
        self.elastic.indices.refresh()

        if self.auto_commit:
            Timer(1, self.run_auto_commit).start()

    def get_last_doc(self):
        """Returns the last document stored in the Elastic engine.
        """
        try:
            result = self.elastic.search(
                index="_all",
                body={
                    "query": {"match_all": {}},
                    "sort": [{"_ts":"desc"}]
                },
                size=1
            )["hits"]["hits"]
        except (es_exceptions.ConnectionError):
            raise errors.ConnectionFailed("Could not connect to Elastic Search")
        except es_exceptions.TransportError:
            raise errors.OperationFailed(
                "Could not retrieve last document from Elastic Search")

        return result[0]["_source"] if len(result) > 0 else None
