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

    This file is a document manager for MongoDB, but the intent
    is that this file can be used as an example to add on different backends.
    To extend this to other systems, simply implement the exact same class and
    replace the method definitions with API calls for the desired backend.
    """

import pymongo
import sys
import time

try:
    import simplejson as json
except:
    import json


class DocManager():
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using MongoDB native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, unique_key='_id'):
        """ Verify URL and establish a connection.
        """
        try:
            self.mongo = pymongo.Connection(url)
        except pymongo.errors.InvalidURI:
            raise SystemError
        self.unique_key = unique_key

    def stop(self):
        """Stops any running threads
        """
        pass

    def upsert(self, doc):
        """Update or insert a document into Mongo
        """
        db, coll = doc['ns'].split('.', 1)
        self.mongo[db][coll].save(doc)

    def remove(self, doc):
        """Removes document from Mongo

        The input is a python dictionary that represents a mongo document.
        The documents has ns and _ts fields.
        """
        db, coll = doc['ns'].split('.', 1)
        self.mongo[db][coll].remove({self.unique_key: doc[self.unique_key]})

    def search(self, start_ts, end_ts):
        """Called to query Mongo for documents in a time range.
        """
        search_set = []
        db_list = self.mongo.database_names()
        for db in db_list:
            if db == "config" or db == "local":
                continue
            coll_list = self.mongo[db].collection_names()
            for coll in coll_list:
                if coll.startswith("system"):
                    continue
                namespace = str(db) + "." + str(coll)
                search_set.append(namespace)

        res = []
        for namespace in search_set:
            db, coll = namespace.split('.', 1)
            target_coll = self.mongo[db][coll]
            res.extend(list(target_coll.find({'_ts': {'$lte': end_ts,
                                                      '$gte': start_ts}})))

        return res

    def commit(self):
        return

    def get_last_doc(self):
        """Returns the last document stored in Mongo.
        """
        search_set = []
        db_list = self.mongo.database_names()
        for db in db_list:
            if db == "config" or db == "local":
                continue
            coll_list = self.mongo[db].collection_names()
            for coll in coll_list:
                if coll.startswith("system"):
                    continue
                namespace = str(db) + "." + str(coll)
                search_set.append(namespace)

        res = []
        for namespace in search_set:
            db, coll = namespace.split('.', 1)
            target_coll = self.mongo[db][coll]
            res.extend(list(target_coll.find().sort('_ts', -1)))

        max_ts = 0
        max_doc = None
        for it in res:
            if it['_ts'] > max_ts:
                max_ts = it['_ts']
                max_doc = it

        return max_doc

    def _remove(self):
        """For test purposes only. Removes all documents in test.test
        """
        self.mongo['test']['test'].remove()

    def _search(self):
        """For test purposes only. Performs search on Elastic with empty query.
        Does not have to be implemented.
        """
        return list(self.mongo['test']['test'].find())
