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

"""Receives documents from the oplog worker threads and indexes them
    into the backend.

    This file is a document manager for the Elastic search engine, but the
    intent is that this file can be used as an example to add on different
    backends. To extend this to other systems, simply implement the exact
    same class and replace the method definitions with API calls for the
    desired backend.
    """

import sys

from pyes import ES, ESRange, RangeQuery, MatchAllQuery
from threading import Timer
from util import verify_url, retry_until_ok
from bson.objectid import ObjectId
import simplejson as json
from util import bson_ts_to_long


class DocManager():
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using elastic native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, auto_commit=True, unique_key='_id'):
        """Verify Elastic URL and establish a connection.
        """

        if verify_url(url) is False:
            raise SystemError
        self.elastic = ES(server=url)
        self.auto_commit = auto_commit
        self.doc_type = 'string'  # default type is string, change if needed
        self.unique_key = unique_key
        if auto_commit:
            self.run_auto_commit()

    def stop(self):
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
        self.elastic.index(doc, index, doc_type, doc_id)

    def remove(self, doc):
        """Removes documents from Elastic

        The input is a python dictionary that represents a mongo document.
        """
        try:
            self.elastic.delete(doc['ns'], 'string', str(doc[self.unique_key]))
        except:
            pass

    def _remove(self):
        """For test purposes only. Removes all documents in test.test
        """
        try:
            self.elastic.delete('test.test', 'string', '')
        except:
            pass

    def search(self, start_ts, end_ts):
        """Called to query Elastic for documents in a time range.
        """
        res = ESRange('_ts', from_value=start_ts, to_value=end_ts)
        q = RangeQuery(res)
        results = self.elastic.search(q)
        return results

    def _search(self):
        """For test purposes only. Performs search on Elastic with empty query.
        Does not have to be implemented.
        """
        results = self.elastic.search(MatchAllQuery())
        return results

    def commit(self):
        """This function is used to force a refresh/commit.
        """
        retry_until_ok(self.elastic.refresh)

    def run_auto_commit(self):
        """Periodically commits to the Elastic server.
        """
        self.elastic.refresh()

        if self.auto_commit:
            Timer(1, self.run_auto_commit).start()

    def get_last_doc(self):
        """Returns the last document stored in the Elastic engine.
        """

        it = None
        q = MatchAllQuery()
        result = self.elastic.search(q, size=1, sort={'_ts:desc'})
        for it in result:
            r = it
            break
        return r
