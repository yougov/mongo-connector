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

This file is a document manager for the Solr search engine, but the intent
is that this file can be used as an example to add on different backends.
To extend this to other systems, simply implement the exact same class and
replace the method definitions with API calls for the desired backend.
"""
import re
import json

from pysolr import Solr, SolrError

from mongo_connector import errors
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import retry_until_ok
from mongo_connector.doc_managers import DocManagerBase, exception_wrapper
from mongo_connector.doc_managers.formatters import DocumentFlattener


# pysolr only has 1 exception: SolrError
wrap_exceptions = exception_wrapper({
    SolrError: errors.OperationFailed})

ADMIN_URL = 'admin/luke?show=schema&wt=json'

decoder = json.JSONDecoder()


class DocManager(DocManagerBase):
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK, **kwargs):
        """Verify Solr URL and establish a connection.
        """
        self.solr = Solr(url)
        self.unique_key = unique_key
        # pysolr does things in milliseconds
        if auto_commit_interval is not None:
            self.auto_commit_interval = auto_commit_interval * 1000
        else:
            self.auto_commit_interval = None
        self.chunk_size = chunk_size
        self.field_list = []
        self._build_fields()
        self._formatter = DocumentFlattener()

    def _parse_fields(self, result, field_name):
        """ If Schema access, parse fields and build respective lists
        """
        field_list = []
        for key, value in result.get('schema', {}).get(field_name, {}).items():
            if key not in field_list:
                field_list.append(key)
        return field_list

    @wrap_exceptions
    def _build_fields(self):
        """ Builds a list of valid fields
        """
        declared_fields = self.solr._send_request('get', ADMIN_URL)
        result = decoder.decode(declared_fields)
        self.field_list = self._parse_fields(result, 'fields')

        # Build regular expressions to match dynamic fields.
        # dynamic field names may have exactly one wildcard, either at
        # the beginning or the end of the name
        self._dynamic_field_regexes = []
        for wc_pattern in self._parse_fields(result, 'dynamicFields'):
            if wc_pattern[0] == "*":
                self._dynamic_field_regexes.append(
                    re.compile(".*%s\Z" % wc_pattern[1:]))
            elif wc_pattern[-1] == "*":
                self._dynamic_field_regexes.append(
                    re.compile("\A%s.*" % wc_pattern[:-1]))

    def _clean_doc(self, doc):
        """Reformats the given document before insertion into Solr.

        This method reformats the document in the following ways:
          - removes extraneous fields that aren't defined in schema.xml
          - unwinds arrays in order to find and later flatten sub-documents
          - flattens the document so that there are no sub-documents, and every
            value is associated with its dot-separated path of keys

        An example:
          {"a": 2,
           "b": {
             "c": {
               "d": 5
             }
           },
           "e": [6, 7, 8]
          }

        becomes:
          {"a": 2, "b.c.d": 5, "e.0": 6, "e.1": 7, "e.2": 8}

        """

        # Translate the _id field to whatever unique key we're using.
        # _id may not exist in the doc, if we retrieved it from Solr
        # as part of update.
        if '_id' in doc:
            doc[self.unique_key] = str(doc.pop("_id"))

        # SOLR cannot index fields within sub-documents, so flatten documents
        # with the dot-separated path to each value as the respective key
        flat_doc = self._formatter.format_document(doc)

        # Only include fields that are explicitly provided in the
        # schema or match one of the dynamic field patterns, if
        # we were able to retrieve the schema
        if len(self.field_list) + len(self._dynamic_field_regexes) > 0:
            def include_field(field):
                return field in self.field_list or any(
                    regex.match(field) for regex in self._dynamic_field_regexes
                )
            return dict((k, v) for k, v in flat_doc.items() if include_field(k))
        return flat_doc

    def stop(self):
        """ Stops the instance
        """
        pass

    def apply_update(self, doc, update_spec):
        """Override DocManagerBase.apply_update to have flat documents."""
        # Replace a whole document
        if not '$set' in update_spec and not '$unset' in update_spec:
            # update spec contains the new document
            update_spec['_ts'] = doc['_ts']
            update_spec['ns'] = doc['ns']
            update_spec['_id'] = doc['_id']
            return update_spec
        for to_set in update_spec.get("$set", []):
            value = update_spec['$set'][to_set]
            # Find dotted-path to the value, remove that key from doc, then
            # put value at key:
            keys_to_pop = []
            for key in doc:
                if key.startswith(to_set):
                    if key == to_set or key[len(to_set)] == '.':
                        keys_to_pop.append(key)
            for key in keys_to_pop:
                doc.pop(key)
            doc[to_set] = value
        for to_unset in update_spec.get("$unset", []):
            # MongoDB < 2.5.2 reports $unset for fields that don't exist within
            # the document being updated.
            keys_to_pop = []
            for key in doc:
                if key.startswith(to_unset):
                    if key == to_unset or key[len(to_unset)] == '.':
                        keys_to_pop.append(key)
            for key in keys_to_pop:
                doc.pop(key)
        return doc

    @wrap_exceptions
    def update(self, doc, update_spec):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        # Commit outstanding changes so that the document to be updated is the
        # same version to which the changes apply.
        self.commit()
        query = "%s:%s" % (self.unique_key, str(doc['_id']))
        results = self.solr.search(query)
        if not len(results):
            # Document may not be retrievable yet
            self.commit()
            results = self.solr.search(query)
        # Results is an iterable containing only 1 result
        for doc in results:
            updated = self.apply_update(doc, update_spec)
            # A _version_ of 0 will always apply the update
            updated['_version_'] = 0
            self.upsert(updated)
            return updated

    @wrap_exceptions
    def upsert(self, doc):
        """Update or insert a document into Solr

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """
        if self.auto_commit_interval is not None:
            self.solr.add([self._clean_doc(doc)],
                          commit=(self.auto_commit_interval == 0),
                          commitWithin=str(self.auto_commit_interval))
        else:
            self.solr.add([self._clean_doc(doc)], commit=False)

    @wrap_exceptions
    def bulk_upsert(self, docs):
        """Update or insert multiple documents into Solr

        docs may be any iterable
        """
        if self.auto_commit_interval is not None:
            add_kwargs = {
                "commit": (self.auto_commit_interval == 0),
                "commitWithin": str(self.auto_commit_interval)
            }
        else:
            add_kwargs = {"commit": False}

        cleaned = (self._clean_doc(d) for d in docs)
        if self.chunk_size > 0:
            batch = list(next(cleaned) for i in range(self.chunk_size))
            while batch:
                self.solr.add(batch, **add_kwargs)
                batch = list(next(cleaned)
                             for i in range(self.chunk_size))
        else:
            self.solr.add(cleaned, **add_kwargs)

    @wrap_exceptions
    def remove(self, doc):
        """Removes documents from Solr

        The input is a python dictionary that represents a mongo document.
        """
        self.solr.delete(id=str(doc["_id"]),
                         commit=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def _remove(self):
        """Removes everything
        """
        self.solr.delete(q='*:*', commit=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def _stream_search(self, query):
        """Helper method for iterating over Solr search results."""
        for doc in self.solr.search(query, rows=100000000):
            if self.unique_key != "_id":
                doc["_id"] = doc.pop(self.unique_key)
            yield doc

    @wrap_exceptions
    def search(self, start_ts, end_ts):
        """Called to query Solr for documents in a time range."""
        query = '_ts: [%s TO %s]' % (start_ts, end_ts)
        return self._stream_search(query)

    @wrap_exceptions
    def _search(self, query):
        """For test purposes only. Performs search on Solr with given query
            Does not have to be implemented.
        """
        return self._stream_search(query)

    def commit(self):
        """This function is used to force a commit.
        """
        retry_until_ok(self.solr.commit)

    @wrap_exceptions
    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        try:
            result = self.solr.search('*:*', sort='_ts desc', rows=1)
        except ValueError:
            return None

        for r in result:
            r['_id'] = r.pop(self.unique_key)
            return r
