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

"""Test mongo using the synchronizer, i.e. as it would be used by an
    user
"""

import os
import sys
import time

from bson import SON
from gridfs import GridFS

sys.path[0:0] = [""]  # noqa

from mongo_connector.doc_managers.mongo_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from mongo_connector.test_utils import (
    ReplicaSet,
    ReplicaSetSingle,
    Server,
    connector_opts,
    assert_soon,
    close_client,
)

from tests import unittest, SkipTest
from mongo_connector.version import Version


class MongoTestCase(unittest.TestCase):

    use_single_meta_collection = False

    @classmethod
    def setUpClass(cls):
        cls.standalone = Server().start()
        cls.mongo_doc = DocManager(cls.standalone.uri)
        cls.mongo_conn = cls.standalone.client()
        cls.desination_version = Version.from_client(cls.mongo_conn)
        cls.mongo = cls.mongo_conn["test"]["test"]

    @classmethod
    def tearDownClass(cls):
        close_client(cls.mongo_conn)
        cls.standalone.stop()

    def _search(self, **kwargs):
        for doc in self.mongo.find(**kwargs):
            yield doc

        fs = GridFS(self.mongo_conn["test"], "test")

        collection_name = "test.test"
        if self.use_single_meta_collection:
            collection_name = "__oplog"
        col = self.mongo_conn["__mongo_connector"][collection_name]
        for doc in col.find():
            if doc.get("gridfs_id"):
                for f in fs.find({"_id": doc["gridfs_id"]}):
                    doc["filename"] = f.filename
                    doc["content"] = f.read()
                    yield doc

    def _remove(self):
        for db in self.mongo_conn.database_names():
            if db not in ["local", "admin"]:
                self.mongo_conn.drop_database(db)


class MongoReplicaSetTestCase(MongoTestCase):
    def setUp(self):
        self.repl_set = self.replica_set_class().start()
        self.conn = self.repl_set.client()

        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        self._remove()
        self.connector = Connector(
            mongo_address=self.repl_set.uri,
            doc_managers=(self.mongo_doc,),
            namespace_options={
                "test.test": {"gridfs": True},
                "rename.me": "new.target",
                "rename.me2": "new2.target2",
            },
            **connector_opts
        )

        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        assert_soon(lambda: sum(1 for _ in self._search()) == 0)

    def drop_all_databases(self):
        for name in self.mongo_conn.database_names():
            if name not in ["local", "admin"]:
                self.mongo_conn.drop_database(name)
        for name in self.conn.database_names():
            if name not in ["local", "admin"]:
                self.conn.drop_database(name)

    def tearDown(self):
        self.connector.join()
        self.drop_all_databases()
        self.repl_set.stop()


class TestMongoReplicaSetSingle(MongoReplicaSetTestCase):
    """ Tests MongoDB to MongoDB DocManager replication with a 1 node replica
    set.
    """

    replica_set_class = ReplicaSetSingle

    def test_insert(self):
        """Tests insert
        """

        self.conn["test"]["test"].insert_one({"name": "paulie"})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)
        result_set_1 = self._search()
        self.assertEqual(sum(1 for _ in result_set_1), 1)
        result_set_2 = self.conn["test"]["test"].find_one()
        for item in result_set_1:
            self.assertEqual(item["_id"], result_set_2["_id"])
            self.assertEqual(item["name"], result_set_2["name"])

    def test_remove(self):
        """Tests remove
        """

        self.conn["test"]["test"].insert_one({"name": "paulie"})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)
        self.conn["test"]["test"].delete_one({"name": "paulie"})
        assert_soon(lambda: sum(1 for _ in self._search()) != 1)
        self.assertEqual(sum(1 for _ in self._search()), 0)

    def test_insert_file(self):
        """Tests inserting a gridfs file
        """
        fs = GridFS(self.conn["test"], "test")
        test_data = b"test_insert_file test file"
        id = fs.put(test_data, filename="test.txt", encoding="utf8")
        assert_soon(lambda: sum(1 for _ in self._search()) > 0)

        res = list(self._search())
        self.assertEqual(len(res), 1)
        doc = res[0]
        self.assertEqual(doc["filename"], "test.txt")
        self.assertEqual(doc["_id"], id)
        self.assertEqual(doc["content"], test_data)

    def test_remove_file(self):
        fs = GridFS(self.conn["test"], "test")
        id = fs.put("test file", filename="test.txt", encoding="utf8")
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)
        fs.delete(id)
        assert_soon(lambda: sum(1 for _ in self._search()) == 0)

    def test_update(self):
        """Test update operations."""
        # Insert
        self.conn.test.test.insert_one({"a": 0})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)

        def check_update(update_spec):
            updated = self.conn.test.command(
                SON(
                    [
                        ("findAndModify", "test"),
                        ("query", {"a": 0}),
                        ("update", update_spec),
                        ("new", True),
                    ]
                )
            )["value"]

            def update_worked():
                replicated = self.mongo_doc.mongo.test.test.find_one({"a": 0})
                return replicated == updated

            # Allow some time for update to propagate
            assert_soon(update_worked)

        # Update by adding a field
        check_update({"$set": {"b": [{"c": 10}, {"d": 11}]}})

        # Update by setting an attribute of a sub-document beyond end of array.
        check_update({"$set": {"b.10.c": 42}})

        # Update by un-setting an array element.
        check_update({"$unset": {"b.10": True}})

        # Update by un-setting a non-existent attribute.
        check_update({"$unset": {"not-present": True}})

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

        # Update by adding new bucket to list
        check_update({"$push": {"b": {"e": 12}}})

        # Update by changing an entire sub-document
        check_update({"$set": {"b.0": {"e": 4}}})

        # Update by adding a sub-document
        check_update({"$set": {"b": {"0": {"c": 100}}}})

        # Update whole document
        check_update({"a": 0, "b": {"1": {"d": 10000}}})

    def check_renamed_insert(self, target_coll):
        target_db, target_coll = target_coll.split(".", 1)
        mongo_target = self.mongo_conn[target_db][target_coll]
        assert_soon(lambda: len(list(mongo_target.find({}))))
        target_docs = list(mongo_target.find({}))
        self.assertEqual(len(target_docs), 1)
        self.assertEqual(target_docs[0]["renamed"], 1)

    def create_renamed_collection(self, source_coll, target_coll):
        """Create renamed collections for the command tests."""
        # Create the rename database and 'rename.me' collection
        source_db, source_coll = source_coll.split(".", 1)
        mongo_source = self.conn[source_db][source_coll]
        mongo_source.insert_one({"renamed": 1})
        self.check_renamed_insert(target_coll)

    def test_drop_database_renamed(self):
        """Test the dropDatabase command on a renamed database."""
        if not self.desination_version.at_least(3, 0, 7) or (
            self.desination_version.at_least(3, 1)
            and not self.desination_version.at_least(3, 1, 9)
        ):
            raise SkipTest("This test fails often because of SERVER-13212")
        self.create_renamed_collection("rename.me", "new.target")
        self.create_renamed_collection("rename.me2", "new2.target2")
        # test that drop database removes target databases
        self.conn.drop_database("rename")
        assert_soon(lambda: "new" not in self.mongo_conn.database_names())
        assert_soon(lambda: "new2" not in self.mongo_conn.database_names())

    def test_drop_collection_renamed(self):
        """Test the drop collection command on a renamed collection."""
        self.create_renamed_collection("rename.me", "new.target")
        self.create_renamed_collection("rename.me2", "new2.target2")
        # test that drop collection removes target collection
        self.conn.rename.drop_collection("me")
        assert_soon(lambda: "target" not in self.mongo_conn.new.collection_names())
        self.conn.rename.drop_collection("me2")
        assert_soon(lambda: "target2" not in self.mongo_conn.new2.collection_names())

    def test_rename_collection_renamed(self):
        """Test the renameCollection command on a renamed collection to a
        renamed collection.
        """
        self.create_renamed_collection("rename.me", "new.target")
        self.conn.admin.command("renameCollection", "rename.me", to="rename.me2")
        # In the target, 'new.target' should be renamed to 'new2.target2'
        assert_soon(lambda: "target" not in self.mongo_conn.new.collection_names())
        self.check_renamed_insert("new2.target2")


class TestMongoReplicaSet(MongoReplicaSetTestCase):
    """ Tests MongoDB to MongoDB DocManager replication with a 3 node replica
    set.
    """

    replica_set_class = ReplicaSet

    def test_rollback(self):
        """Tests rollback. We force a rollback by adding a doc, killing the
        primary, adding another doc, killing the new primary, and then
        restarting both.
        """
        primary_conn = self.repl_set.primary.client()
        self.conn["test"]["test"].insert_one({"name": "paul"})
        condition = (
            lambda: self.conn["test"]["test"].find_one({"name": "paul"}) is not None
        )
        assert_soon(condition)
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)

        self.repl_set.primary.stop(destroy=False)
        new_primary_conn = self.repl_set.secondary.client()
        admin = new_primary_conn["admin"]

        def condition():
            return admin.command("isMaster")["ismaster"]

        assert_soon(lambda: retry_until_ok(condition))

        retry_until_ok(self.conn.test.test.insert_one, {"name": "pauline"})
        assert_soon(lambda: sum(1 for _ in self._search()) == 2)
        result_set_1 = list(self._search())
        result_set_2 = self.conn["test"]["test"].find_one({"name": "pauline"})
        self.assertEqual(len(result_set_1), 2)
        # make sure pauline is there
        for item in result_set_1:
            if item["name"] == "pauline":
                self.assertEqual(item["_id"], result_set_2["_id"])
        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()
        assert_soon(lambda: primary_conn["admin"].command("isMaster")["ismaster"])

        self.repl_set.secondary.start()

        time.sleep(2)
        result_set_1 = list(self._search())
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item["name"], "paul")
        find_cursor = retry_until_ok(self.conn["test"]["test"].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)


if __name__ == "__main__":
    unittest.main()
