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
from threading import Lock
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK, DEFAULT_CATEGORIZER,
                                       DEFAULT_INDEX_CATEGORY, DEFAULT_INDEX_NAME_PREFIX)
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
        es_nodes = [node.strip(' ') for node in url.split(',')]
        self.elastic = Elasticsearch(hosts=es_nodes, **kwargs.get('clientOptions', {}))
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
        self.refresh_interval = None
        self.index_created = False
        self.alias_added = False
        self.mutex = Lock()

    def _index_and_mapping(self, namespace):
        """Helper method for getting the index and type from a namespace."""
        index, doc_type = namespace.split('.', 1)
        return DEFAULT_INDEX_NAME_PREFIX + index.lower(), doc_type

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

    def index_alias_add(self, mapped_ns, namespace):
        with self.mutex:
            if not self.alias_added:
                index_alias, doc_type_alias = self._index_and_mapping(namespace)
                index, doc_type = self._index_and_mapping(mapped_ns)
                alias_name = index_alias.replace(DEFAULT_INDEX_NAME_PREFIX, "", 1)
                try:
                    self.__remove_alias_and_index(alias_name)
                    self.elastic.indices.put_alias(index=index, name=alias_name)
                    LOG.info("Added alias %s for index %s" % (alias_name, index))
                    self.alias_added = True
                except Exception, e:
                    LOG.critical("Failed to add alias for index %s due to error %r" % (index, e))
            else:
                LOG.info("Alias already added for namespace: %s, skipping alias creation" % namespace)

    def __remove_alias_and_index(self, alias_name):
        try:
            alias_dict = self.elastic.indices.get_alias(name=alias_name)
            for index_name in alias_dict:
                LOG.info("Removing alias %s from index %s" % (alias_name, index_name))
                self.elastic.indices.delete_alias(index=index_name, name=alias_name)
        except es_exceptions.NotFoundError:
            LOG.info("No alias with name %s found" % alias_name)
        try:
            if self.elastic.indices.exists(index=alias_name):
                LOG.info("Found index with name: %s, deleting old index to set new alias" % alias_name)
                self.elastic.indices.delete(index=alias_name)
                LOG.warning("Deleted index with name: %s" % alias_name)
        except es_exceptions.RequestError, e:
            LOG.error("Failed to delete index with name: %s due to error: %r" % (alias_name, e))

    def index_create(self, namespace):
        with self.mutex:
            if not self.index_created:
                index, doc_type = self._index_and_mapping(namespace)
                request = {'settings': {'index': self.categorizer[self.index_category]}}
                LOG.info("Creating index: %r with settings: %r" % (index, request))
                try:
                    self.elastic.indices.create(index=index, body=request)
                    self.index_created = True
                except es_exceptions.RequestError, e:
                    LOG.warning('Failed to create index due to error: %r' % e)
            else:
                LOG.info("Index already created, skipping index creation")

    def disable_refresh(self, namespace):
        with self.mutex:
            if not self.refresh_interval:
                try:
                    index_name, doc_type_name = self._index_and_mapping(namespace)
                    old_settings = self.elastic.indices.get_settings(index=index_name)[index_name]['settings']
                    self.refresh_interval = old_settings.get('index', {}).get('refresh_interval', '30s')
                    self.elastic.indices.put_settings(body={'index': {'refresh_interval': '-1'}}, index=index_name)
                    LOG.info("Bulk Upsert: Setting refresh interval to -1, old interval: %r" % self.refresh_interval)
                except Exception, e:
                    LOG.warning("Exception %r encountered while disabling refresh on index" % e)
        return self.refresh_interval

    def enable_refresh(self, namespace):
        with self.mutex:
            if self.refresh_interval:
                try:
                    index_name, doc_type_name = self._index_and_mapping(namespace)
                    self.elastic.indices.put_settings(body={'index': {'refresh_interval': self.refresh_interval}}, index=index_name)
                    LOG.info("Bulk Upsert: Resetting refresh interval back to %s" % self.refresh_interval)
                    self.refresh_interval = None
                except Exception:
                    LOG.warning("Bulk Upsert: Failed to refresh interval back to %s" % self.refresh_interval)

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
        self.mapGeoFields(doc)
        doc_id = u(doc.get("_id"))

        try:
            # Index the source document, using lowercase namespace as index name.
            self.elastic.index(index=index, doc_type=doc_type,
                               body=self._formatter.format_document(doc), id=doc_id,
                               refresh=(self.auto_commit_interval == 0))
        except es_exceptions.RequestError, e:
            LOG.info("Failed to upsert document: %r", e.info)
            error = self.parseError(e.info['error'])
            if(error):
                return (doc_id, error['field_name'])
        return None

    def mapGeoFields(self, doc):
        for key in doc.keys():
            if key == 'geo' or key.startswith('moe_geo'):
                value = doc.pop(key)
            else:
                continue
            try:
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    mapped_value = {'lat': value[1], 'lon': value[0]}
                    doc[key] = mapped_value
                elif isinstance(value, dict) and 'lat' in value and 'lon' in value:
                    doc[key] = value
            except:
                LOG.warning("Incorrect value passed in a geo point field: %r, value: %r" % (key, value))

    def parseError(self, errorDesc):
        parsed = search("MapperParsingException[{}[{field_name}]{}", errorDesc)
        if not parsed:
            parsed = search("MapperParsingException[{}[{}]{}[{field_name}]{}", errorDesc)
        if not parsed:
            parsed = search("{}MapperParsingException[{}[{field_name}]{}", errorDesc)
        if not parsed:
            parsed = search("{}MapperParsingException[{}[{}]{}[{field_name}]{}", errorDesc)
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
                self.mapGeoFields(doc)
                doc_id = u(doc.get("_id"))
                document_action = {
                    "_index": index_name,
                    "_type": doc_type,
                    "_id": doc_id,
                    "_source": self._formatter.format_document(doc)
                }
                yield document_action
        responses = []
        doc_actions = docs_to_upsert()
        if not doc_actions:
            return
        try:
            kw = {}
            if self.chunk_size > 0:
                kw['chunk_size'] = self.chunk_size
            responses = streaming_bulk(client=self.elastic,
                                       actions=doc_actions,
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
            pass
        except Exception, e:
            LOG.critical("Bulk Upsert: Failed due to error: %r" % e)
            raise

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        doc = f.get_metadata()
        self.mapGeoFields(doc)
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
