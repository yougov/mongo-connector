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

    This file is a document manager for MongoDB, but the intent
    is that this file can be used as an example to add on different backends.
    To extend this to other systems, simply implement the exact same class and
    replace the method definitions with API calls for the desired backend.
    """

import pymongo
from mongo_connector import errors
from bson.errors import InvalidDocument

class DocManager():
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using MongoDB native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, unique_key='_id', **kwargs):
        """ Verify URL and establish a connection.
        """
        try:
            self.mongo = pymongo.MongoClient(url)
        except pymongo.errors.InvalidURI:
            raise errors.ConnectionFailed("Invalid URI for MongoDB")
        except pymongo.errors.ConnectionFailure:
            raise errors.ConnectionFailed("Failed to connect to MongoDB")
        self.unique_key = unique_key
        self.namespace_set = kwargs.get("namespace_set")

    def _namespaces(self):
        """Provides the list of namespaces being replicated to MongoDB
        """
        if self.namespace_set:
            return self.namespace_set

        user_namespaces = []
        db_list = self.mongo.database_names()
        for database in db_list:
            if database == "config" or database == "local":
                continue
            coll_list = self.mongo[database].collection_names()
            for coll in coll_list:
                if coll.startswith("system"):
                    continue
                namespace = str(database) + "." + str(coll)
                user_namespaces.append(namespace)
        return user_namespaces

    def stop(self):
        """Stops any running threads
        """
        pass

    def upsert(self, doc):
        """Update or insert a document into Mongo
        """
        database, coll = doc['ns'].split('.', 1)
        try:
            self.mongo[database][coll].save(doc)
        except pymongo.errors.OperationFailure:
            raise errors.OperationFailed("Could not complete upsert on MongoDB")
        except InvalidDocument:
            raise errors.OperationFailed("Cannot insert invalid doc to MongoDB")

    def remove(self, doc):
        """Removes document from Mongo

        The input is a python dictionary that represents a mongo document.
        The documents has ns and _ts fields.
        """
        database, coll = doc['ns'].split('.', 1)
        self.mongo[database][coll].remove(
            {self.unique_key: doc[self.unique_key]})

    def search(self, start_ts, end_ts):
        """Called to query Mongo for documents in a time range.
        """
        for namespace in self._namespaces():
            database, coll = namespace.split('.', 1)
            target_coll = self.mongo[database][coll]
            for document in target_coll.find({'_ts': {'$lte': end_ts,
                                                      '$gte': start_ts}}):
                yield document

    def commit(self):
        """ Performs a commit
        """
        return

    def get_last_doc(self):
        """Returns the last document stored in Mongo.
        """
        def docs_by_ts():
            for namespace in self._namespaces():
                database, coll = namespace.split('.', 1)
                target_coll = self.mongo[database][coll]
                for doc in target_coll.find(limit=1).sort('_ts', -1):
                    yield doc

        return max(docs_by_ts(), key=lambda x:x["_ts"])

    def _remove(self):
        """For test purposes only. Removes all documents in test.test
        """
        self.mongo['test']['test'].remove()

    def _search(self):
        """For test purposes only. Performs search on MongoDB with empty query.
        Does not have to be implemented.
        """
        return self.mongo['test']['test'].find()
