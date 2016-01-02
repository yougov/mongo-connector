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
import base64
import logging

from threading import Timer

from parse import search

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import streaming_bulk

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK, DEFAULT_CATEGORIZER, DEFAULT_INDEX_CATEGORY)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

wrap_exceptions = exception_wrapper({
    es_exceptions.ConnectionError: errors.ConnectionFailed,
    es_exceptions.TransportError: errors.OperationFailed,
    es_exceptions.NotFoundError: errors.OperationFailed,
    es_exceptions.RequestError: errors.OperationFailed})

LOG = logging.getLogger(__name__)

tracer = logging.getLogger('elasticsearch.trace')
tracer.setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


class DocManager(DocManagerBase):
    """Elasticsearch implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    Elasticsearch.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK,
                 attachment_field="content", categorizer=DEFAULT_CATEGORIZER,
                 index_category=DEFAULT_INDEX_CATEGORY, **kwargs):
        self.elastic = Elasticsearch(
            hosts=[url], **kwargs.get('clientOptions', {}))
        self.auto_commit_interval = auto_commit_interval
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.categorizer = categorizer
        self.index_category = index_category
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()
        self._formatter = DefaultDocumentFormatter()

        self.has_attachment_mapping = False
        self.attachment_field = attachment_field

    def _index_and_mapping(self, namespace):
        """Helper method for getting the index and type from a namespace."""
        index, doc_type = namespace.split('.', 1)
        return index.lower(), doc_type

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    def index_exists(self, namespace):
        index, doc_type = self._index_and_mapping(namespace)
        try:
            return self.elastic.indices.exists(index=index)
        except Exception:
            return False

    def index_create(self, namespace):
        index, doc_type = self._index_and_mapping(namespace)
        request = {'settings': {'index': self.categorizer[self.index_category]}}
        LOG.info("Creating index: %r with settings: %r" % (index, request))
        try:
            self.elastic.indices.create(index=index, body=request)
        except es_exceptions.RequestError, e:
            LOG.warning('Failed to create index due to error: %r' % e)

    def disable_refresh(self, namespace):
        refresh_interval = '30s'
        try:
            index_name, doc_type_name = self._index_and_mapping(namespace)
            old_settings = self.elastic.indices.get_settings(index=index_name)[index_name]['settings']
            old_refresh_interval = old_settings.get('index', {}).get('refresh_interval', '30s')
            if old_refresh_interval is not '-1':
                refresh_interval = old_refresh_interval
            self.elastic.indices.put_settings(body={'index': {'refresh_interval': '-1'}}, index=index_name)
            LOG.info("Bulk Upsert: Setting refresh interval to -1, old interval: %r" % refresh_interval)
        except Exception:
            pass
        return refresh_interval

    def enable_refresh(self, namespace, refresh_interval='30s'):
        index_name, doc_type_name = self._index_and_mapping(namespace)
        try:
            self.elastic.indices.put_settings(body={'index': {'refresh_interval': refresh_interval}}, index=index_name)
            LOG.info("Bulk Upsert: Resetting refresh interval back to %s" % refresh_interval)
        except Exception:
            LOG.warning("Bulk Upsert: Failed to refresh interval back to %s" % refresh_interval)
            pass

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        if doc.get('dropDatabase'):
            dbs = self.command_helper.map_db(db)
            for _db in dbs:
                self.elastic.indices.delete(index=_db.lower())

        if doc.get('renameCollection'):
            LOG.debug("elastic_doc_manager does not support renaming a mapping")

        if doc.get('create'):
            db, coll = self.command_helper.map_collection(db, doc['create'])
            if db and coll:
                self.elastic.indices.put_mapping(
                    index=db.lower(), doc_type=coll,
                    body={
                        "_source": {"enabled": True}
                    })

        if doc.get('drop'):
            db, coll = self.command_helper.map_collection(db, doc['drop'])
            if db and coll:
                self.elastic.indices.delete_mapping(index=db.lower(),
                                                    doc_type=coll)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp, doc=None):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """
        index, doc_type = self._index_and_mapping(namespace)
        document = {}
        if not doc:
            try:
                document = self.elastic.get(index=index, doc_type=doc_type, id=u(document_id))
            except es_exceptions.NotFoundError, e:
                return (document_id, e)
        else:
            document['_source'] = doc
        updated = self.apply_update(document['_source'], update_spec)
        # _id is immutable in MongoDB, so won't have changed in update
        updated['_id'] = document['_id']
        error = self.upsert(updated, namespace)
        if error:
            return error
        return (updated['_id'], None)

    @wrap_exceptions
    def upsert(self, doc, namespace):
        """Insert a document into Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)
        # No need to duplicate '_id' in source document
        doc_id = u(doc.get("_id"))
        try:
            # Index the source document, using lowercase namespace as index name.
            self.elastic.index(index=index, doc_type=doc_type,
                               body=self._formatter.format_document(doc), id=doc_id,
                               refresh=(self.auto_commit_interval == 0),
                               request_timeout=300)
        except es_exceptions.RequestError, e:
            LOG.info("Failed to upsert document: %r", e.info)
            error = self.parseError(e.info['error'])
            if(error):
                return (doc_id, error['field_name'])
        return None

    def parseError(self, errorDesc):
        parsed = search("MapperParsingException[{}[{field_name}]{}", errorDesc)
        if not parsed:
            parsed = search("MapperParsingException[{}[{}]{}[{field_name}]{}", errorDesc)
        LOG.info("Parsed ES Error: %s from description %s", parsed, errorDesc)
        if parsed and parsed.named:
            return parsed.named
        LOG.warning("Couldn't parse ES error: %s", errorDesc)
        return None

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        """Insert multiple documents into Elasticsearch."""
        index_name, doc_type = self._index_and_mapping(namespace)

        def docs_to_upsert():
            doc = None
            for doc in docs:
                doc_id = u(doc.get("_id"))
                document_action = {
                    "_index": index_name,
                    "_type": doc_type,
                    "_id": doc_id,
                    "_source": self._formatter.format_document(doc)
                }
                yield document_action
            if doc is None:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search")
        responses = []
        try:
            kw = {'request_timeout': 300}
            if self.chunk_size > 0:
                kw['chunk_size'] = self.chunk_size
            responses = streaming_bulk(client=self.elastic,
                                       actions=docs_to_upsert(),
                                       **kw)
            docs_inserted = 0
            for ok, resp in responses:
                if not ok and resp:
                    try:
                        index = resp['index']
                        error_field = self.parseError(index['error'])
                        error = (index['_id'], error_field['field_name'])
                        LOG.info("Found failed document from bulk upsert: %s", error)
                        yield error
                    except Exception:
                        LOG.error("Could not parse response to reinsert: %r" % resp)
                else:
                    docs_inserted += 1
                    if(docs_inserted % 10000 == 0):
                        LOG.info("Bulk Upsert: Inserted %d docs" % docs_inserted)
            LOG.info("Bulk Upsert: Finished inserting %d docs" % docs_inserted)
            self.commit(index_name)
        except errors.EmptyDocsError:
            # This can happen when mongo-connector starts up, there is no
            # config file, but nothing to dump
            pass
        except Exception, e:
            LOG.critical("Bulk Upsert: Failed due to error: %r" % e)
            pass

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        doc = f.get_metadata()
        doc_id = str(doc.get('_id'))
        index, doc_type = self._index_and_mapping(namespace)

        # make sure that elasticsearch treats it like a file
        if not self.has_attachment_mapping:
            body = {
                "properties": {
                    self.attachment_field: {"type": "attachment"}
                }
            }
            self.elastic.indices.put_mapping(index=index,
                                             doc_type=doc_type,
                                             body=body)
            self.has_attachment_mapping = True

        doc = self._formatter.format_document(doc)
        doc[self.attachment_field] = base64.b64encode(f.read()).decode()

        self.elastic.index(index=index, doc_type=doc_type,
                           body=doc, id=doc_id,
                           refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Remove a document from Elasticsearch."""
        index, doc_type = self._index_and_mapping(namespace)
        self.elastic.delete(index=index, doc_type=doc_type,
                            id=u(document_id),
                            refresh=(self.auto_commit_interval == 0))

    def commit(self, index_name=None):
        """Refresh all Elasticsearch indexes."""
        if not index_name:
            retry_until_ok(self.elastic.indices.refresh, index="")
        else:
            retry_until_ok(self.elastic.indices.refresh, index=index_name)

    def run_auto_commit(self):
        """Periodically commit to the Elastic server."""
        self.elastic.indices.refresh()
        if self.auto_commit_interval not in [None, 0]:
            Timer(self.auto_commit_interval, self.run_auto_commit).start()
