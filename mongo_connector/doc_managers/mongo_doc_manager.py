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

import logging
import pymongo

from mongo_connector import errors
from mongo_connector.util import exception_wrapper
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

wrap_exceptions = exception_wrapper({
    pymongo.errors.ConnectionFailure: errors.ConnectionFailed,
    pymongo.errors.OperationFailure: errors.OperationFailed})

LOG = logging.getLogger(__name__)


class DocManager(DocManagerBase):
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using MongoDB native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, **kwargs):
        """ Verify URL and establish a connection.
        """
        try:
            self.mongo = pymongo.MongoClient(url)
        except pymongo.errors.InvalidURI:
            raise errors.ConnectionFailed("Invalid URI for MongoDB")
        except pymongo.errors.ConnectionFailure:
            raise errors.ConnectionFailed("Failed to connect to MongoDB")
        self.namespace_set = kwargs.get("namespace_set")
        for namespace in self._namespaces():
            self.mongo["__mongo_connector"][namespace].create_index("_ts")

    @wrap_exceptions
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
                namespace = "%s.%s" % (database, coll)
                user_namespaces.append(namespace)
        return user_namespaces

    def stop(self):
        """Stops any running threads
        """
        LOG.info(
            "Mongo DocManager Stopped: If you will not target this system "
            "again with mongo-connector then please drop the database "
            "__mongo_connector in order to return resources to the OS."
        )

    @wrap_exceptions
    def update(self, doc, update_spec):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        db, coll = doc['ns'].split('.', 1)
        updated = self.mongo[db][coll].find_and_modify(
            {'_id': doc['_id']},
            update_spec,
            new=True
        )
        return updated

    @wrap_exceptions
    def upsert(self, doc):
        """Update or insert a document into Mongo
        """
        database, coll = doc['ns'].split('.', 1)
        ts = doc.pop("_ts")
        ns = doc.pop("ns")

        self.mongo["__mongo_connector"][ns].save({
            '_id': doc['_id'],
            "_ts": ts,
            "ns": ns
        })
        self.mongo[database][coll].save(doc)

    @wrap_exceptions
    def remove(self, doc):
        """Removes document from Mongo

        The input is a python dictionary that represents a mongo document.
        The documents has ns and _ts fields.
        """
        database, coll = doc['ns'].split('.', 1)
        self.mongo[database][coll].remove({'_id': doc["_id"]})
        self.mongo["__mongo_connector"][doc['ns']].remove({'_id': doc["_id"]})

    @wrap_exceptions
    def search(self, start_ts, end_ts):
        """Called to query Mongo for documents in a time range.
        """
        for namespace in self._namespaces():
            database, coll = namespace.split('.', 1)
            for ts_ns_doc in self.mongo["__mongo_connector"][namespace].find(
                {'_ts': {'$lte': end_ts,
                         '$gte': start_ts}}
            ):
                yield ts_ns_doc

    def commit(self):
        """ Performs a commit
        """
        return

    @wrap_exceptions
    def get_last_doc(self):
        """Returns the last document stored in Mongo.
        """
        def docs_by_ts():
            for namespace in self._namespaces():
                database, coll = namespace.split('.', 1)
                mc_coll = self.mongo["__mongo_connector"][namespace]
                for ts_ns_doc in mc_coll.find(limit=1).sort('_ts', -1):
                    yield ts_ns_doc

        return max(docs_by_ts(), key=lambda x: x["_ts"])

    @wrap_exceptions
    def _remove(self):
        """For test purposes only. Removes all documents in test.test
        """
        self.mongo['test']['test'].remove()

    @wrap_exceptions
    def _search(self):
        """For test purposes only. Performs search on MongoDB with empty query.
        Does not have to be implemented.
        """
        return self.mongo['test']['test'].find()
