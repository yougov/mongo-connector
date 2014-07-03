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

"""Elasticsearch implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
Elasticsearch.
"""
import logging
from threading import Timer

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import scan, streaming_bulk

from mongo_connector import errors
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import retry_until_ok
from mongo_connector.doc_managers import DocManagerBase, exception_wrapper
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter


wrap_exceptions = exception_wrapper({
    es_exceptions.ConnectionError: errors.ConnectionFailed,
    es_exceptions.TransportError: errors.OperationFailed})


class DocManager(DocManagerBase):
    """Elasticsearch implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    Elasticsearch.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK, **kwargs):
        self.elastic = Elasticsearch(hosts=[url])
        self.auto_commit_interval = auto_commit_interval
        self.doc_type = 'string'  # default type is string, change if needed
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()
        self._formatter = DefaultDocumentFormatter()

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    @wrap_exceptions
    def update(self, doc, update_spec):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
	TODO: We should be able to do an intial get and figure out whether
        _source is enabled or not.  If _source is enabled, we can avoid a get call 
        which will improve update performance.
        """
        document = self.elastic.get(index=doc['ns'],
                                    id=str(doc['_id']))
        update = None
        if not "_source" in document:
            updated = self.apply_update(document, update_spec)
            updated['ns'] = doc['ns']
            updated['_ts'] = doc['_ts']
        else:
            updated = self.apply_update(document['_source'], update_spec)
	    updated['_doc_op_'] = 'u'
        # _id is immutable in MongoDB, so won't have changed in update
        updated['_id'] = document['_id']

        self.upsert(updated)
        return updated

    @wrap_exceptions
    def upsert(self, doc):
        """Insert a document into Elasticsearch."""
        doc_type = self.doc_type
        index = doc['ns']
        # No need to duplicate '_id' in source document
        doc_id = str(doc.pop("_id"))
        
        # check for operation
        operation = None
        if not "_doc_op_" in doc:
            operation = 'i'
        else:
            operation = doc.pop("_doc_op_")

	updbody = {}
        if operation == 'u':
            updbody['doc']=self._formatter.format_document(doc)
            self.elastic.update(index=index, doc_type=doc_type, id=doc_id,
                                body=updbody,
                                refresh=(self.auto_commit_interval == 0))
        else:
            self.elastic.index(index=index, doc_type=doc_type,
                               body=self._formatter.format_document(doc), id=doc_id,
                               refresh=(self.auto_commit_interval == 0))
        # Don't mutate doc argument
        doc['_id'] = doc_id

    @wrap_exceptions
    def bulk_upsert(self, docs):
        """Insert multiple documents into Elasticsearch."""
        def docs_to_upsert():
            doc = None
            for doc in docs:
                index = doc["ns"]
                doc_id = str(doc.pop("_id"))
                yield {
                    "_index": index,
                    "_type": self.doc_type,
                    "_id": doc_id,
                    "_source": self._formatter.format_document(doc)
                }
            if not doc:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search")
        try:
            kw = {}
            if self.chunk_size > 0:
                kw['chunk_size'] = self.chunk_size

            responses = streaming_bulk(client=self.elastic,
                                       actions=docs_to_upsert(),
                                       **kw)

            for ok, resp in responses:
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
        """Remove a document from Elasticsearch."""
        self.elastic.delete(index=doc['ns'], doc_type=self.doc_type,
                            id=str(doc["_id"]),
                            refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results."""
        for hit in scan(self.elastic, query=kwargs.pop('body', None),
                        scroll='10m', **kwargs):
            hit['_source']['_id'] = hit['_id']
            yield hit['_source']

    def search(self, start_ts, end_ts):
        """Query Elasticsearch for documents in a time range.

        This method is used to find documents that may be in conflict during
        a rollback event in MongoDB.
        """
        return self._stream_search(
            index="_all",
            body={
                "query": {
                    "filtered": {
                        "filter": {
                            "range": {
                                "_ts": {"gte": start_ts, "lte": end_ts}
                            }
                        }
                    }
                }
            })

    def commit(self):
        """Refresh all Elasticsearch indexes."""
        retry_until_ok(self.elastic.indices.refresh, index="")

    def run_auto_commit(self):
        """Periodically commit to the Elastic server."""
        self.elastic.indices.refresh()
        if self.auto_commit_interval not in [None, 0]:
            Timer(self.auto_commit_interval, self.run_auto_commit).start()

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document from Elasticsearch.

        This method is used to help define a time window within which documents
        may be in conflict after a MongoDB rollback.
        """
        try:
            result = self.elastic.search(
                index="_all",
                body={
                    "query": {"match_all": {}},
                    "sort": [{"_ts": "desc"}],
                },
                size=1
            )["hits"]["hits"]
            for r in result:
                r['_source']['_id'] = r['_id']
                return r['_source']
        except es_exceptions.RequestError:
            # no documents so ES returns 400 because of undefined _ts mapping
            return None
