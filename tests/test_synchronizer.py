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

"""Test synchronizer using DocManagerSimulator
"""

import os
import sys
import time

sys.path[0:0] = [""]  # noqa

from mongo_connector.connector import Connector
from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.test_utils import assert_soon, connector_opts, ReplicaSetSingle
from tests import unittest


class TestSynchronizer(unittest.TestCase):
    """ Tests the synchronizers
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the cluster
        """
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        open("oplog.timestamp", "w").close()

        cls.repl_set = ReplicaSetSingle().start()
        cls.conn = cls.repl_set.client()
        cls.connector = Connector(
            mongo_address=cls.repl_set.uri, ns_set=["test.test"], **connector_opts
        )
        cls.synchronizer = cls.connector.doc_managers[0]
        cls.connector.start()
        assert_soon(lambda: len(cls.connector.shard_set) != 0)

    @classmethod
    def tearDownClass(cls):
        """ Tears down connector
        """
        cls.connector.join()
        cls.repl_set.stop()

    def setUp(self):
        """ Clears the db
        """
        self.conn["test"]["test"].delete_many({})
        assert_soon(lambda: len(self.synchronizer._search()) == 0)

    def test_insert(self):
        """Tests insert
        """
        self.conn["test"]["test"].insert_one({"name": "paulie"})
        while len(self.synchronizer._search()) == 0:
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn["test"]["test"].find_one()
        for item in result_set_1:
            self.assertEqual(item["_id"], result_set_2["_id"])
            self.assertEqual(item["name"], result_set_2["name"])

    def test_ns_set(self):
        self.conn.test.other.insert_one({"replicated": False})
        results = self.synchronizer._search()
        self.assertEqual(
            len(results), 0, "Should not replicate outside of test.test namespace"
        )

    def test_remove(self):
        """Tests remove
        """
        self.conn["test"]["test"].insert_one({"name": "paulie"})
        while len(self.synchronizer._search()) != 1:
            time.sleep(1)
        self.conn["test"]["test"].delete_one({"name": "paulie"})

        while len(self.synchronizer._search()) == 1:
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 0)

    def test_update(self):
        """Test that Connector can replicate updates successfully."""
        doc = {"a": 1, "b": 2}
        self.conn.test.test.insert_one(doc)
        selector = {"_id": doc["_id"]}

        def update_and_retrieve(update_spec, replace=False):
            if replace:
                self.conn.test.test.replace_one(selector, update_spec)
            else:
                self.conn.test.test.update_one(selector, update_spec)

            # self.conn.test.test.update(selector, update_spec)
            # Give the connector some time to perform update
            time.sleep(1)
            return self.synchronizer._search()[0]

        # Update whole document
        doc = update_and_retrieve({"a": 1, "b": 2, "c": 10}, replace=True)
        self.assertEqual(doc["a"], 1)
        self.assertEqual(doc["b"], 2)
        self.assertEqual(doc["c"], 10)

        # $set only
        doc = update_and_retrieve({"$set": {"b": 4}})
        self.assertEqual(doc["a"], 1)
        self.assertEqual(doc["b"], 4)

        # $unset only
        doc = update_and_retrieve({"$unset": {"a": True}})
        self.assertNotIn("a", doc)
        self.assertEqual(doc["b"], 4)

        # mixed $set/$unset
        doc = update_and_retrieve({"$unset": {"b": True}, "$set": {"c": 3}})
        self.assertEqual(doc["c"], 3)
        self.assertNotIn("b", doc)

        # ensure update works when fields are given
        opthread = self.connector.shard_set[0]
        opthread.namespace_config = NamespaceConfig(include_fields=["a", "b", "c"])
        try:
            doc = update_and_retrieve({"$set": {"d": 10}})
            self.assertEqual(self.conn.test.test.find_one(doc["_id"])["d"], 10)
            self.assertNotIn("d", doc)
            doc = update_and_retrieve({"$set": {"a": 10}})
            self.assertEqual(doc["a"], 10)
        finally:
            # cleanup
            opthread.fields = None


if __name__ == "__main__":
    unittest.main()
