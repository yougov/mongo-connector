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

This file is a document manager for the Solr search engine, but the intent
is that this file can be used as an example to add on different backends.
To extend this to other systems, simply implement the exact same class and
replace the method definitions with API calls for the desired backend.
Each method is detailed to describe the desired behavior.
"""
#!/usr/env/python
import sys

from pysolr import Solr
from threading import Timer
from util import verify_url, retry_until_ok


class DocManager():
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, auto_commit=True, unique_key='_id'):
        """Verify Solr URL and establish a connection.

        This method may vary from implementation to implementation, but it
        should verify the url to the backend and return None if that fails.
        It should also create the connection to the backend, and start a
        periodic committer if necessary. The Solr uniqueKey is '_id' in
        the sample schema, but this may be overridden by user defined
        configuration.
        """
        if verify_url(url) is False:
            print("Invalid Solr URL")
            self.solr = None
            return None

        self.solr = Solr(url)
        self.unique_key = unique_key
        self.auto_commit = auto_commit

        if auto_commit:
            self.run_auto_commit()

    def stop(self):
        self.auto_commit = False

    def upsert(self, doc):
        """Update or insert a document into Solr

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """
        self.solr.add([doc], commit=False)

    def remove(self, doc):
        """Removes documents from Solr

        The input is a python dictionary that represents a mongo document.
        """
        self.solr.delete(id=str(doc[self.unique_key]), commit=False)

    def search(self, start_ts, end_ts):
        """Called to query Solr for documents in a time range.

        This method is only used by rollbacks to query all the documents in
        Solr within a certain timestamp window. The input will be two longs
        (converted from Bson timestamp) which specify the time range. The
        return value should be an iterable set of documents.
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

        It is used only in the beginning of rollbacks and in test cases, and is
        not meant to be called in other circumstances. The body should commit
        all documents to the backend engine (like auto_commit), but not have
        any timers or run itself again (unlike auto_commit). In the event of
        too many Solr searchers, the commit is wrapped in a retry_until_ok to
        keep trying until the commit goes through.
        """
        retry_until_ok(self.solr.commit)

    def run_auto_commit(self):
        """Periodically commits to the Solr server.

        This function commits all changes to the Solr engine, and then starts a
        timer that calls this function again in one second. The reason for this
        function is to prevent overloading Solr from other searchers. This
        function may be modified based on the backend engine and how commits
        are handled, as timers may not be necessary in all instances.
        """
        self.solr.commit()
        if self.auto_commit:
            Timer(1, self.run_auto_commit).start()

    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.

        This method is used for rollbacks to establish the rollback window,
        which is the gap between the last document on a mongo shard and the
        last document in Solr. If there are no documents, this functions
        returns None. Otherwise, it returns the first document.
        """
        #search everything, sort by descending timestamp, return 1 row
        result = self.solr.search('*:*', sort='_ts desc', rows=1
                                  )

        if len(result) == 0:
            return None

        return result.docs[0]
