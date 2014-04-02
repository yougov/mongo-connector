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

import bson.json_util as bsjson
from pysolr import Solr, SolrError
from mongo_connector import errors
from mongo_connector.constants import DEFAULT_COMMIT_INTERVAL
from mongo_connector.util import retry_until_ok
ADMIN_URL = 'admin/luke?show=schema&wt=json'

decoder = json.JSONDecoder()


class DocManager():
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', **kwargs):
        """Verify Solr URL and establish a connection.
        """
        self.solr = Solr(url)
        self.unique_key = unique_key
        # pysolr does things in milliseconds
        if auto_commit_interval is not None:
            self.auto_commit_interval = auto_commit_interval * 1000
        else:
            self.auto_commit_interval = None
        self.field_list = []
        self._build_fields()

    def _parse_fields(self, result, field_name):
        """ If Schema access, parse fields and build respective lists
        """
        field_list = []
        for key, value in result.get('schema', {}).get(field_name, {}).items():
            if key not in field_list:
                field_list.append(key)
        return field_list

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
        # SOLR cannot index fields within sub-documents, so flatten documents
        # with the dot-separated path to each value as the respective key
        def flattened(doc):
            def flattened_kernel(doc, path):
                for k, v in doc.items():
                    path.append(k)
                    if isinstance(v, dict):
                        for inner_k, inner_v in flattened_kernel(v, path):
                            yield inner_k, inner_v
                    elif isinstance(v, list):
                        for li, lv in enumerate(v):
                            path.append(str(li))
                            if isinstance(lv, dict):
                                for dk, dv in flattened_kernel(lv, path):
                                    yield dk, dv
                            else:
                                yield ".".join(path), lv
                            path.pop()
                    else:
                        yield ".".join(path), v
                    path.pop()
            return dict(flattened_kernel(doc, []))

        # Translate the _id field to whatever unique key we're using
        doc[self.unique_key] = doc["_id"]
        flat_doc = flattened(doc)

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

    def upsert(self, doc):
        """Update or insert a document into Solr

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """
        try:
            if self.auto_commit_interval is not None:
                self.solr.add([self._clean_doc(doc)],
                              commit=(self.auto_commit_interval == 0),
                              commitWithin=str(self.auto_commit_interval))
            else:
                self.solr.add([self._clean_doc(doc)], commit=False)
        except SolrError:
            raise errors.OperationFailed(
                "Could not insert %r into Solr" % bsjson.dumps(doc))

    def bulk_upsert(self, docs):
        """Update or insert multiple documents into Solr

        docs may be any iterable
        """
        try:
            cleaned = (self._clean_doc(d) for d in docs)
            if self.auto_commit_interval is not None:
                self.solr.add(cleaned, commit=(self.auto_commit_interval == 0),
                              commitWithin=str(self.auto_commit_interval))
            else:
                self.solr.add(cleaned, commit=False)
        except SolrError:
            raise errors.OperationFailed(
                "Could not bulk-insert documents into Solr")

    def remove(self, doc):
        """Removes documents from Solr

        The input is a python dictionary that represents a mongo document.
        """
        self.solr.delete(id=str(doc[self.unique_key]),
                         commit=(self.auto_commit_interval == 0))

    def _remove(self):
        """Removes everything
        """
        self.solr.delete(q='*:*', commit=(self.auto_commit_interval == 0))

    def search(self, start_ts, end_ts):
        """Called to query Solr for documents in a time range.
        """
        query = '_ts: [%s TO %s]' % (start_ts, end_ts)
        return self.solr.search(query, rows=100000000)

    def _search(self, query):
        """For test purposes only. Performs search on Solr with given query
            Does not have to be implemented.
        """
        return self.solr.search(query, rows=200)

    def commit(self):
        """This function is used to force a commit.
        """
        retry_until_ok(self.solr.commit)

    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        try:
            result = self.solr.search('*:*', sort='_ts desc', rows=1)
        except ValueError:
            return None

        if len(result) == 0:
            return None

        return result.docs[0]
