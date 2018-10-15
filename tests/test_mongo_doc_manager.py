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

"""Tests each of the functions in mongo_doc_manager
"""

import sys

sys.path[0:0] = [""]  # noqa

from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.mongo_doc_manager import DocManager
from mongo_connector.test_utils import TESTARGS, MockGridFSFile
from tests import unittest
from tests.test_mongo import MongoTestCase


class TestMongoDocManager(MongoTestCase):
    """Test class for MongoDocManager
    """

    id_field = "_id"

    @classmethod
    def setUpClass(cls):
        MongoTestCase.setUpClass()
        cls.namespaces_inc = ["test.test_include1", "test.test_include2"]

    def setUp(self):
        """Empty Mongo at the start of every test
        """

        self.choosy_docman = DocManager(
            self.standalone.uri,
            use_single_meta_collection=self.use_single_meta_collection,
        )

        self.mongo_conn.drop_database("__mongo_connector")
        self._remove()

        conn = self.standalone.client()
        for ns in self.namespaces_inc:
            db, coll = ns.split(".", 1)
            conn[db][coll].drop()

    def test_meta_collections(self):
        """Ensure that a DocManager returns the correct set of meta collection
        names.
        """
        meta_collection_names = set()
        if self.use_single_meta_collection:
            meta_collection_names.add(self.choosy_docman.meta_collection_name)
        # Before replication only the single meta_collection should be
        # returned.
        self.assertEqual(
            set(self.choosy_docman._meta_collections()), meta_collection_names
        )
        for namespace in ["test.test", "foo.bar", "test.bar"]:
            if not self.use_single_meta_collection:
                meta_collection_names.add(namespace)
            self.choosy_docman.upsert({"_id": "1"}, namespace, 1)
            self.assertEqual(
                set(self.choosy_docman._meta_collections()), meta_collection_names
            )

    def test_update(self):
        doc_id = "1"
        doc = {"_id": doc_id, "a": 1, "b": 2}
        self.mongo.insert_one(doc)
        # $set only
        update_spec = {"$set": {"a": 1, "b": 2}}
        doc = self.choosy_docman.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": doc_id, "a": 1, "b": 2})
        # $unset only
        update_spec = {"$unset": {"a": True}}
        doc = self.choosy_docman.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": doc_id, "b": 2})
        # mixed $set/$unset
        update_spec = {"$unset": {"b": True}, "$set": {"c": 3}}
        doc = self.choosy_docman.update(doc_id, update_spec, *TESTARGS)
        self.assertEqual(doc, {"_id": doc_id, "c": 3})

    def test_upsert(self):
        """Ensure we can properly insert into Mongo via DocManager.
        """

        docc = {"_id": "1", "name": "John"}
        self.choosy_docman.upsert(docc, *TESTARGS)
        res = list(self._search())
        self.assertEqual(len(res), 1)
        for doc in res:
            self.assertEqual(doc["_id"], "1")
            self.assertEqual(doc["name"], "John")

        docc = {"_id": "1", "name": "Paul"}
        self.choosy_docman.upsert(docc, *TESTARGS)
        res = list(self._search())
        self.assertEqual(len(res), 1)
        for doc in res:
            self.assertEqual(doc["_id"], "1")
            self.assertEqual(doc["name"], "Paul")

    def test_bulk_upsert(self):
        """Test the bulk_upsert method."""
        docs = ({"_id": i} for i in range(1000))
        self.choosy_docman.bulk_upsert(docs, *TESTARGS)
        res = list(self._search(sort=[("_id", 1)]))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r["_id"], i)

        docs = ({"_id": i, "weight": 2 * i} for i in range(1000))
        self.choosy_docman.bulk_upsert(docs, *TESTARGS)

        res = list(self._search(sort=[("_id", 1)]))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r["weight"], 2 * i)

    def test_remove(self):
        """Ensure we can properly delete from Mongo via DocManager.
        """

        docc = {"_id": "1", "name": "John"}
        self.choosy_docman.upsert(docc, *TESTARGS)
        self.assertEqual(len(list(self._search())), 1)
        self.choosy_docman.remove(docc["_id"], *TESTARGS)
        self.assertEqual(len(list(self._search())), 0)

    def test_insert_file(self):
        # Drop database, so that mongo_doc's client refreshes its index cache.
        self.choosy_docman.mongo.drop_database("test")
        test_data = " ".join(str(x) for x in range(100000)).encode("utf8")
        docc = {
            "_id": "test_id",
            "filename": "test_filename",
            "upload_date": 5,
            "md5": "test_md5",
        }
        self.choosy_docman.insert_file(MockGridFSFile(docc, test_data), *TESTARGS)
        res = self._search()
        for doc in res:
            self.assertEqual(doc[self.id_field], docc["_id"])
            self.assertEqual(doc["filename"], docc["filename"])
            self.assertEqual(doc["content"], test_data)

    def test_remove_file(self):
        # Drop database, so that mongo_doc's client refreshes its index cache.
        self.choosy_docman.mongo.drop_database("test")
        test_data = b"hello world"
        docc = {
            "_id": "test_id",
            "filename": "test_filename",
            "upload_date": 5,
            "md5": "test_md5",
        }

        self.choosy_docman.insert_file(MockGridFSFile(docc, test_data), *TESTARGS)
        res = list(self._search())
        self.assertEqual(len(res), 1)

        self.choosy_docman.remove(docc["_id"], *TESTARGS)
        res = list(self._search())
        self.assertEqual(len(res), 0)

    def test_search(self):
        """Query Mongo for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """

        docc = {"_id": "1", "name": "John"}
        self.choosy_docman.upsert(docc, "test.test_include1", 5767301236327972865)
        docc2 = {"_id": "2", "name": "John Paul"}
        self.choosy_docman.upsert(docc2, "test.test_include1", 5767301236327972866)
        docc3 = {"_id": "3", "name": "Paul"}
        self.choosy_docman.upsert(docc3, "test.test_include1", 5767301236327972870)
        search = list(
            self.choosy_docman.search(5767301236327972865, 5767301236327972866)
        )
        self.assertEqual(len(search), 2)
        result_id = [result.get(self.id_field) for result in search]
        self.assertIn("1", result_id)
        self.assertIn("2", result_id)

    def test_search_namespaces(self):
        """Test search within timestamp range with a given namespace set
        """

        for ns in self.namespaces_inc:
            for i in range(100):
                self.choosy_docman.upsert({"_id": i}, ns, i)

        results = list(self.choosy_docman.search(0, 49))
        self.assertEqual(len(results), 100)
        for r in results:
            self.assertGreaterEqual(r[self.id_field], 0)

    def test_get_last_doc(self):
        """Insert documents, verify that get_last_doc() returns the one with
            the latest timestamp.
        """
        docc = {"_id": "4", "name": "Hare"}
        self.choosy_docman.upsert(docc, "test.test_include1", 3)
        docc = {"_id": "5", "name": "Tortoise"}
        self.choosy_docman.upsert(docc, "test.test_include1", 2)
        docc = {"_id": "6", "name": "Mr T."}
        self.choosy_docman.upsert(docc, "test.test_include1", 1)
        doc = self.choosy_docman.get_last_doc()
        self.assertEqual(doc[self.id_field], "4")
        docc = {"_id": "6", "name": "HareTwin"}
        self.choosy_docman.upsert(docc, "test.test_include1", 4)
        doc = self.choosy_docman.get_last_doc()
        self.assertEqual(doc[self.id_field], "6")

    def test_commands(self):
        # Also test with namespace mapping.
        # Note that mongo-connector does not currently support commands after
        # renaming a database.
        namespace_config = NamespaceConfig(
            namespace_set=["test.test", "test.test2", "test.drop"],
            namespace_options={
                "test.test": "test.othertest",
                "test.drop": "dropped.collection",
            },
        )
        self.choosy_docman.command_helper = CommandHelper(namespace_config)

        try:
            self.choosy_docman.handle_command({"create": "test"}, *TESTARGS)
            self.assertIn("othertest", self.mongo_conn["test"].collection_names())
            self.choosy_docman.handle_command(
                {"renameCollection": "test.test", "to": "test.test2"}, "admin.$cmd", 1
            )
            self.assertNotIn("othertest", self.mongo_conn["test"].collection_names())
            self.assertIn("test2", self.mongo_conn["test"].collection_names())

            self.choosy_docman.handle_command({"drop": "test2"}, "test.$cmd", 1)
            self.assertNotIn("test2", self.mongo_conn["test"].collection_names())

            # WiredTiger drops the database when the last collection is
            # dropped.
            if "test" not in self.mongo_conn.database_names():
                self.choosy_docman.handle_command({"create": "test"}, *TESTARGS)
            self.assertIn("test", self.mongo_conn.database_names())
            self.choosy_docman.handle_command({"dropDatabase": 1}, "test.$cmd", 1)
            self.assertNotIn("test", self.mongo_conn.database_names())

            # Briefly test mapped database name with dropDatabase command.
            self.mongo_conn.dropped.collection.insert_one({"a": 1})
            self.assertIn("dropped", self.mongo_conn.database_names())
            self.choosy_docman.handle_command({"dropDatabase": 1}, "test.$cmd", 1)
            self.assertNotIn("dropped", self.mongo_conn.database_names())
        finally:
            self.mongo_conn.drop_database("test")


class TestMongoDocManagerWithSingleMetaCollection(TestMongoDocManager):
    id_field = "doc_id"
    use_single_meta_collection = True


if __name__ == "__main__":
    unittest.main()
