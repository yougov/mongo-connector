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

from bson import SON
from gridfs import GridFS

from mongo_connector import errors, constants
from mongo_connector.util import exception_wrapper
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

wrap_exceptions = exception_wrapper(
    {
        pymongo.errors.ConnectionFailure: errors.ConnectionFailed,
        pymongo.errors.OperationFailure: errors.OperationFailed,
    }
)

LOG = logging.getLogger(__name__)

__version__ = constants.__version__
"""MongoDB DocManager version information

This is packaged with mongo-connector so it shares the same version.
Downstream DocManager implementations should add their package __version__
string here, for example:

__version__ = '0.1.0'
"""


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
            self.mongo = pymongo.MongoClient(url, **kwargs.get("clientOptions", {}))
        except pymongo.errors.InvalidURI:
            raise errors.ConnectionFailed("Invalid URI for MongoDB")
        except pymongo.errors.ConnectionFailure:
            raise errors.ConnectionFailed("Failed to connect to MongoDB")
        self.chunk_size = kwargs.get("chunk_size", constants.DEFAULT_MAX_BULK)
        self.use_single_meta_collection = kwargs.get(
            "use_single_meta_collection", False
        )
        self.meta_collection_name = kwargs.get(
            "meta_collection_name", constants.DEFAULT_META_COLLECTION_NAME
        )
        self.meta_collection_cap_size = kwargs.get(
            "meta_collection_cap_size", constants.DEFAULT_META_COLLECTION_CAP_SIZE
        )

        # The '_id' field has to be unique, so if we will be writing data from
        # different namespaces into single collection, we use a different field
        # for storing the document id.
        self.id_field = "doc_id" if self.use_single_meta_collection else "_id"
        self.meta_database = self.mongo["__mongo_connector"]

        # Create the meta collection as capped if a single meta collection is
        # preferred
        if self.use_single_meta_collection:
            if self.meta_collection_name not in self.meta_database.collection_names():
                self.meta_database.create_collection(
                    self.meta_collection_name,
                    capped=True,
                    size=self.meta_collection_cap_size,
                )
                meta_collection = self.meta_database[self.meta_collection_name]
                meta_collection.create_index(self.id_field)
                meta_collection.create_index([("ns", 1), ("_ts", 1)])

    def _db_and_collection(self, namespace):
        return namespace.split(".", 1)

    def _get_meta_collection(self, namespace):
        if self.use_single_meta_collection:
            return self.meta_collection_name
        else:
            return namespace

    @wrap_exceptions
    def _meta_collections(self):
        """Provides the meta collections currently being used
        """
        if self.use_single_meta_collection:
            yield self.meta_collection_name
        else:
            for name in self.meta_database.collection_names(
                include_system_collections=False
            ):
                yield name

    def stop(self):
        """Stops any running threads
        """
        LOG.info(
            "Mongo DocManager Stopped: If you will not target this system "
            "again with mongo-connector then you may drop the database "
            "__mongo_connector, which holds metadata for Mongo Connector."
        )

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        db, _ = self._db_and_collection(namespace)
        if doc.get("dropDatabase"):
            for new_db in self.command_helper.map_db(db):
                self.mongo.drop_database(new_db)

        if doc.get("renameCollection"):
            a = self.command_helper.map_namespace(doc["renameCollection"])
            b = self.command_helper.map_namespace(doc["to"])
            if a and b:
                self.mongo.admin.command("renameCollection", a, to=b)

        if doc.get("create"):
            new_db, coll = self.command_helper.map_collection(db, doc["create"])
            if new_db:
                self.mongo[new_db].create_collection(coll)

        if doc.get("drop"):
            new_db, coll = self.command_helper.map_collection(db, doc["drop"])
            if new_db:
                self.mongo[new_db].drop_collection(coll)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        db, coll = self._db_and_collection(namespace)

        meta_collection_name = self._get_meta_collection(namespace)

        self.meta_database[meta_collection_name].replace_one(
            {self.id_field: document_id, "ns": namespace},
            {self.id_field: document_id, "_ts": timestamp, "ns": namespace},
            upsert=True,
        )

        no_obj_error = "No matching object found"
        update_spec.pop("$v", None)
        updated = self.mongo[db].command(
            SON(
                [
                    ("findAndModify", coll),
                    ("query", {"_id": document_id}),
                    ("update", update_spec),
                    ("new", True),
                ]
            ),
            allowable_errors=[no_obj_error],
        )["value"]
        return updated

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        """Update or insert a document into Mongo
        """
        database, coll = self._db_and_collection(namespace)

        meta_collection_name = self._get_meta_collection(namespace)

        self.meta_database[meta_collection_name].replace_one(
            {self.id_field: doc["_id"], "ns": namespace},
            {self.id_field: doc["_id"], "_ts": timestamp, "ns": namespace},
            upsert=True,
        )

        self.mongo[database][coll].replace_one({"_id": doc["_id"]}, doc, upsert=True)

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        def iterate_chunks():
            dbname, collname = self._db_and_collection(namespace)
            collection = self.mongo[dbname][collname]
            meta_collection_name = self._get_meta_collection(namespace)
            meta_collection = self.meta_database[meta_collection_name]
            more_chunks = True
            while more_chunks:
                bulk = collection.initialize_ordered_bulk_op()
                bulk_meta = meta_collection.initialize_ordered_bulk_op()
                for i in range(self.chunk_size):
                    try:
                        doc = next(docs)
                        selector = {"_id": doc["_id"]}
                        bulk.find(selector).upsert().replace_one(doc)
                        meta_selector = {self.id_field: doc["_id"]}
                        bulk_meta.find(meta_selector).upsert().replace_one(
                            {
                                self.id_field: doc["_id"],
                                "ns": namespace,
                                "_ts": timestamp,
                            }
                        )
                    except StopIteration:
                        more_chunks = False
                        if i > 0:
                            yield bulk, bulk_meta
                        break
                if more_chunks:
                    yield bulk, bulk_meta

        for bulk_op, meta_bulk_op in iterate_chunks():
            try:
                bulk_op.execute()
                meta_bulk_op.execute()
            except pymongo.errors.DuplicateKeyError as e:
                LOG.warn("Continuing after DuplicateKeyError: " + str(e))
            except pymongo.errors.BulkWriteError as bwe:
                LOG.error(bwe.details)
                raise

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Removes document from Mongo

        The input is a python dictionary that represents a mongo document.
        The documents has ns and _ts fields.
        """
        database, coll = self._db_and_collection(namespace)

        meta_collection = self._get_meta_collection(namespace)

        doc2 = self.meta_database[meta_collection].find_one_and_delete(
            {self.id_field: document_id}
        )
        if doc2 and doc2.get("gridfs_id"):
            GridFS(self.mongo[database], coll).delete(doc2["gridfs_id"])
        else:
            self.mongo[database][coll].delete_one({"_id": document_id})

    @wrap_exceptions
    def insert_file(self, f, namespace, timestamp):
        database, coll = self._db_and_collection(namespace)

        id = GridFS(self.mongo[database], coll).put(f, filename=f.filename)

        meta_collection = self._get_meta_collection(namespace)

        self.meta_database[meta_collection].replace_one(
            {self.id_field: f._id, "ns": namespace},
            {self.id_field: f._id, "_ts": timestamp, "ns": namespace, "gridfs_id": id},
            upsert=True,
        )

    @wrap_exceptions
    def search(self, start_ts, end_ts):
        """Called to query Mongo for documents in a time range.
        """
        for meta_collection_name in self._meta_collections():
            meta_coll = self.meta_database[meta_collection_name]
            for ts_ns_doc in meta_coll.find(
                {"_ts": {"$lte": end_ts, "$gte": start_ts}}
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
            for meta_collection_name in self._meta_collections():
                meta_coll = self.meta_database[meta_collection_name]
                for ts_ns_doc in meta_coll.find(limit=-1).sort("_ts", -1):
                    yield ts_ns_doc

        return max(docs_by_ts(), key=lambda x: x["_ts"])
