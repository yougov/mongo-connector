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

"""Test replication of commands
"""

import sys

sys.path[0:0] = [""]  # noqa

import pymongo

from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.test_utils import assert_soon, close_client, ReplicaSetSingle
from tests import unittest


class CommandLoggerDocManager(DocManagerBase):
    def __init__(self, url=None, **kwargs):
        self.commands = []
        self.command_helper = None

    def stop(self):
        pass

    def upsert(self, doc, namespace, timestamp):
        pass

    def remove(self, document_id, namespace, timestamp):
        pass

    def commit(self):
        pass

    def handle_command(self, doc, namespace, timestamp):
        db, _ = namespace.split(".", 1)
        if doc.get("dropDatabase"):
            if self.command_helper.map_db(db):
                self.commands.append(doc)

        if doc.get("renameCollection"):
            a = self.command_helper.map_namespace(doc["renameCollection"])
            b = self.command_helper.map_namespace(doc["to"])
            if a and b:
                self.commands.append(doc)

        if doc.get("create"):
            new_db, coll = self.command_helper.map_collection(db, doc["create"])
            if new_db:
                self.commands.append(doc)

        if doc.get("drop"):
            new_db, coll = self.command_helper.map_collection(db, doc["drop"])
            if new_db:
                self.commands.append(doc)


class TestCommandReplication(unittest.TestCase):
    def setUp(self):
        self.repl_set = ReplicaSetSingle().start()
        self.primary_conn = self.repl_set.client()
        self.oplog_progress = LockingDict()
        self.opman = None

    def tearDown(self):
        try:
            if self.opman:
                self.opman.join()
        except RuntimeError:
            pass
        close_client(self.primary_conn)
        self.repl_set.stop()

    def initOplogThread(self, namespace_set=None):
        self.docman = CommandLoggerDocManager()
        namespace_config = NamespaceConfig(namespace_set=namespace_set)

        self.docman.command_helper = CommandHelper(namespace_config)
        self.opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(self.docman,),
            oplog_progress_dict=self.oplog_progress,
            namespace_config=namespace_config,
            collection_dump=False,
        )
        self.opman.start()

    def test_command_helper(self):

        mapping = {"a.x": "b.x", "a.y": "c.y"}

        helper = CommandHelper(
            NamespaceConfig(
                namespace_set=list(mapping) + ["a.z"], namespace_options=mapping
            )
        )

        self.assertEqual(set(helper.map_db("a")), set(["a", "b", "c"]))
        self.assertEqual(helper.map_db("d"), [])

        self.assertEqual(helper.map_namespace("a.x"), "b.x")
        self.assertEqual(helper.map_namespace("a.z"), "a.z")
        self.assertEqual(helper.map_namespace("d.x"), None)

        self.assertEqual(helper.map_collection("a", "x"), ("b", "x"))
        self.assertEqual(helper.map_collection("a", "z"), ("a", "z"))
        self.assertEqual(helper.map_collection("d", "x"), (None, None))

    def test_create_collection(self):
        self.initOplogThread()
        pymongo.collection.Collection(self.primary_conn["test"], "test", create=True)
        assert_soon(lambda: self.docman.commands)
        command = self.docman.commands[0]
        self.assertEqual(command["create"], "test")

    def test_create_collection_skipped(self):
        self.initOplogThread(["test.test"])

        pymongo.collection.Collection(self.primary_conn["test2"], "test2", create=True)
        pymongo.collection.Collection(self.primary_conn["test"], "test", create=True)

        assert_soon(lambda: self.docman.commands)
        self.assertEqual(len(self.docman.commands), 1)
        command = self.docman.commands[0]
        self.assertEqual(command["create"], "test")

    def test_drop_collection(self):
        self.initOplogThread()
        coll = pymongo.collection.Collection(
            self.primary_conn["test"], "test", create=True
        )
        coll.drop()
        assert_soon(lambda: len(self.docman.commands) == 2)
        self.assertEqual(self.docman.commands[1], {"drop": "test"})

    def test_drop_database(self):
        self.initOplogThread()
        pymongo.collection.Collection(self.primary_conn["test"], "test", create=True)
        self.primary_conn.drop_database("test")
        assert_soon(lambda: len(self.docman.commands) == 2)
        self.assertEqual(self.docman.commands[1], {"dropDatabase": 1})

    def test_rename_collection(self):
        self.initOplogThread()
        coll = pymongo.collection.Collection(
            self.primary_conn["test"], "test", create=True
        )
        coll.rename("test2")
        assert_soon(lambda: len(self.docman.commands) == 2)
        self.assertEqual(self.docman.commands[1].get("renameCollection"), "test.test")
        self.assertEqual(self.docman.commands[1].get("to"), "test.test2")


if __name__ == "__main__":
    unittest.main()
