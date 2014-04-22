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
import sys
from threading import Timer

import bson.json_util as bsjson
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import bulk

from mongo_connector import errors
from mongo_connector.compat import reraise
from mongo_connector.constants import DEFAULT_COMMIT_INTERVAL
from mongo_connector.util import retry_until_ok


def wrap_exceptions(func):
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except es_exceptions.ConnectionError:
            exc_type, exc_value, exc_tb = sys.exc_info()
            reraise(errors.ConnectionFailed, exc_value, exc_tb)
        except es_exceptions.TransportError:
            exc_type, exc_value, exc_tb = sys.exc_info()
            reraise(errors.OperationFailed, exc_value, exc_tb)
    return wrap


class DocManager():
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using elastic native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=500, **kwargs):
        """ Establish a connection to Elastic
        """
        self.elastic = Elasticsearch(hosts=[url])
        self.auto_commit_interval = auto_commit_interval
        self.doc_type = 'string'  # default type is string, change if needed
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()

    def stop(self):
        """ Stops the instance
        """
        self.auto_commit_interval = None

    @wrap_exceptions
    def upsert(self, doc):
        """Update or insert a document into Elastic

        If you'd like to have different types of document in your database,
        you can store the doc type as a field in Mongo and set doc_type to
        that field. (e.g. doc_type = doc['_type'])

        """
        doc_type = self.doc_type
        index = doc['ns']
        doc[self.unique_key] = str(doc["_id"])
        doc_id = doc[self.unique_key]
        self.elastic.index(index=index, doc_type=doc_type,
                           body=bsjson.dumps(doc), id=doc_id,
                           refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def bulk_upsert(self, docs):
        """Update or insert multiple documents into Elastic

        docs may be any iterable
        """
        def docs_to_upsert():
            doc = None
            for doc in docs:
                index = doc["ns"]
                doc[self.unique_key] = str(doc[self.unique_key])
                doc_id = doc[self.unique_key]
                yield {
                    "_index": index,
                    "_type": self.doc_type,
                    "_id": doc_id,
                    "_source": doc
                }
            if not doc:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search")
        try:
            responses = bulk(client=self.elastic,
                             actions=docs_to_upsert(),
                             chunk_size=self.chunk_size)
            for resp in responses[1]:
                ok = resp['index'].get('ok')
                if ok is None:
                    status = resp['index'].get('status')
                    ok = (300 > status >= 200)
                if not ok:
                    logging.error(
                        "Could not bulk-upsert document "
                        "into ElasticSearch: %r" % resp)
            if self.auto_commit_interval == 0:
                self.commit()
        except errors.EmptyDocsError:
            # This can happen when mongo-connector starts up, there is no
            # config file, but nothing to dump
            pass

    @wrap_exceptions
    def remove(self, doc):
        """Removes documents from Elastic

        The input is a python dictionary that represents a mongo document.
        """
        self.elastic.delete(index=doc['ns'], doc_type=self.doc_type,
                            id=str(doc[self.unique_key]),
                            refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def _remove(self):
        """For test purposes only. Removes all documents in test.test
        """
        self.elastic.delete_by_query(index="test.test",
                                     doc_type=self.doc_type,
                                     q="*:*")
        self.commit()

    @wrap_exceptions
    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results"""
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
        if self.auto_commit_interval not in [None, 0]:
            Timer(self.auto_commit_interval, self.run_auto_commit).start()

    @wrap_exceptions
    def get_last_doc(self):
        """Returns the last document stored in the Elastic engine.
        """
        result = self.elastic.search(
            index="_all",
            body={
                "query": {"match_all": {}},
                "sort": [{"_ts": "desc"}]
            },
            size=1
        )["hits"]["hits"]
        return result[0]["_source"] if len(result) > 0 else None
