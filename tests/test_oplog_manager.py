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

"""Test oplog manager methods
"""

import itertools
import re
import sys
import time

import bson
import gridfs
import pymongo

sys.path[0:0] = [""]  # noqa

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.test_utils import assert_soon, close_client, ReplicaSetSingle
from mongo_connector.util import bson_ts_to_long
from tests import unittest


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """

    def setUp(self):
        self.repl_set = ReplicaSetSingle().start()
        self.primary_conn = self.repl_set.client()
        self.oplog_coll = self.primary_conn.local["oplog.rs"]
        self.opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            namespace_config=NamespaceConfig(
                namespace_options={"test.*": True, "gridfs.*": {"gridfs": True}}
            ),
        )

    def tearDown(self):
        try:
            self.opman.join()
        except RuntimeError:
            pass  # OplogThread may not have been started
        self.primary_conn.drop_database("test")
        close_client(self.primary_conn)
        self.repl_set.stop()

    def test_get_oplog_cursor(self):
        """Test the get_oplog_cursor method"""

        # timestamp is None - all oplog entries excluding no-ops are returned.
        cursor = self.opman.get_oplog_cursor(None)
        self.assertEqual(
            cursor.count(),
            self.primary_conn["local"]["oplog.rs"].find({"op": {"$ne": "n"}}).count(),
        )

        # earliest entry is the only one at/after timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.primary_conn["test"]["test"].insert_one(doc)
        latest_timestamp = self.opman.get_last_oplog_timestamp()
        cursor = self.opman.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count(), 1)
        next_entry_id = next(cursor)["o"]["_id"]
        retrieved = self.primary_conn.test.test.find_one(next_entry_id)
        self.assertEqual(retrieved, doc)

        # many entries before and after timestamp
        self.primary_conn["test"]["test"].insert_many(
            [{"i": i} for i in range(2, 1002)]
        )
        oplog_cursor = self.oplog_coll.find(
            {"op": {"$ne": "n"}, "ns": {"$not": re.compile(r"\.(system|\$cmd)")}},
            sort=[("ts", pymongo.ASCENDING)],
        )

        # initial insert + 1000 more inserts
        self.assertEqual(oplog_cursor.count(), 1 + 1000)
        pivot = oplog_cursor.skip(400).limit(-1)[0]

        goc_cursor = self.opman.get_oplog_cursor(pivot["ts"])
        self.assertEqual(goc_cursor.count(), 1 + 1000 - 400)

    def test_get_last_oplog_timestamp(self):
        """Test the get_last_oplog_timestamp method"""

        # "empty" the oplog
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        self.assertEqual(self.opman.get_last_oplog_timestamp(), None)

        # Test non-empty oplog
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.primary_conn["test"]["test"].insert_one({"i": i + 500})
        oplog = self.primary_conn["local"]["oplog.rs"]
        oplog = oplog.find().sort("$natural", pymongo.DESCENDING).limit(-1)[0]
        self.assertEqual(self.opman.get_last_oplog_timestamp(), oplog["ts"])

    def test_dump_collection(self):
        """Test the dump_collection method

        Cases:

        1. empty oplog
        2. non-empty oplog, with gridfs collections
        3. non-empty oplog, specified a namespace-set, none of the oplog
           entries are for collections in the namespace-set
        """

        # Test with empty oplog
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        last_ts = self.opman.dump_collection()
        self.assertEqual(last_ts, None)

        # Test with non-empty oplog with gridfs collections
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        # Insert 10 gridfs files
        for i in range(10):
            fs = gridfs.GridFS(self.primary_conn["gridfs"], collection="test" + str(i))
            fs.put(b"hello world")
        # Insert 1000 documents
        for i in range(1000):
            self.primary_conn["test"]["test"].insert_one({"i": i + 500})
        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        self.assertEqual(len(self.opman.doc_managers[0]._search()), 1010)

        # Case 3
        # 1MB oplog so that we can rollover quickly
        repl_set = ReplicaSetSingle(oplogSize=1).start()
        conn = repl_set.client()
        opman = OplogThread(
            primary_client=conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            namespace_config=NamespaceConfig(namespace_set=["test.test"]),
        )
        # Insert a document into an included collection
        conn["test"]["test"].insert_one({"test": 1})
        # Cause the oplog to rollover on a non-included collection
        while conn["local"]["oplog.rs"].find_one({"ns": "test.test"}):
            conn["test"]["ignored"].insert_many(
                [{"test": "1" * 1024} for _ in range(1024)]
            )
        last_ts = opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, opman.dump_collection())
        self.assertEqual(len(opman.doc_managers[0]._search()), 1)
        conn.close()
        repl_set.stop()

    def test_skipped_oplog_entry_updates_checkpoint(self):
        repl_set = ReplicaSetSingle().start()
        conn = repl_set.client()
        opman = OplogThread(
            primary_client=conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            namespace_config=NamespaceConfig(namespace_set=["test.test"]),
        )
        opman.start()

        # Insert a document into an included collection
        conn["test"]["test"].insert_one({"test": 1})
        last_ts = opman.get_last_oplog_timestamp()
        assert_soon(
            lambda: last_ts == opman.checkpoint,
            "OplogThread never updated checkpoint to non-skipped " "entry.",
        )
        self.assertEqual(len(opman.doc_managers[0]._search()), 1)

        # Make sure that the oplog thread updates its checkpoint on every
        # oplog entry.
        conn["test"]["ignored"].insert_one({"test": 1})
        last_ts = opman.get_last_oplog_timestamp()
        assert_soon(
            lambda: last_ts == opman.checkpoint,
            "OplogThread never updated checkpoint to skipped entry.",
        )
        opman.join()
        conn.close()
        repl_set.stop()

    def test_dump_collection_with_error(self):
        """Test the dump_collection method with invalid documents.

        Cases:

        1. non-empty oplog, continue_on_error=True, invalid documents
        """

        # non-empty oplog, continue_on_error=True, invalid documents
        self.opman.continue_on_error = True
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]

        docs = [{"a": i} for i in range(100)]
        for i in range(50, 60):
            docs[i]["_upsert_exception"] = True
        self.primary_conn["test"]["test"].insert_many(docs)

        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        docs = self.opman.doc_managers[0]._search()
        docs.sort(key=lambda doc: doc["a"])

        self.assertEqual(len(docs), 90)
        expected_a = itertools.chain(range(0, 50), range(60, 100))
        for doc, correct_a in zip(docs, expected_a):
            self.assertEqual(doc["a"], correct_a)

    def test_dump_collection_cancel(self):
        """Test that dump_collection returns None when cancelled."""
        self.primary_conn["test"]["test"].insert_one({"test": "1"})

        # Pretend that the OplogThead was cancelled
        self.opman.running = False
        self.assertIsNone(self.opman.dump_collection())

    def test_init_cursor(self):
        """Test the init_cursor method

        Cases:

        1. no last checkpoint, no collection dump
        2. no last checkpoint, collection dump ok and stuff to dump
        3. no last checkpoint, nothing to dump, stuff in oplog
        4. no last checkpoint, nothing to dump, nothing in oplog
        5. no last checkpoint, no collection dump, stuff in oplog
        6. last checkpoint exists
        7. last checkpoint is behind
        """

        # N.B. these sub-cases build off of each other and cannot be re-ordered
        # without side-effects

        # No last checkpoint, no collection dump, nothing in oplog
        # "change oplog collection" to put nothing in oplog
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        self.opman.collection_dump = False
        self.assertTrue(all(doc["op"] == "n" for doc in self.opman.init_cursor()[0]))
        self.assertEqual(self.opman.checkpoint, None)

        # No last checkpoint, empty collections, nothing in oplog
        self.opman.collection_dump = True
        cursor, cursor_empty = self.opman.init_cursor()
        self.assertEqual(cursor, None)
        self.assertTrue(cursor_empty)
        self.assertEqual(self.opman.checkpoint, None)

        # No last checkpoint, empty collections, something in oplog
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        collection = self.primary_conn["test"]["test"]
        collection.insert_one({"i": 1})
        collection.delete_one({"i": 1})
        time.sleep(3)
        last_ts = self.opman.get_last_oplog_timestamp()
        cursor, cursor_empty = self.opman.init_cursor()
        self.assertFalse(cursor_empty)
        self.assertEqual(self.opman.checkpoint, last_ts)
        self.assertEqual(self.opman.read_last_checkpoint(), last_ts)

        # No last checkpoint, no collection dump, something in oplog
        # If collection dump is false the checkpoint should not be set
        self.opman.checkpoint = None
        self.opman.oplog_progress = LockingDict()
        self.opman.collection_dump = False
        collection.insert_one({"i": 2})
        cursor, cursor_empty = self.opman.init_cursor()
        for doc in cursor:
            last_doc = doc
        self.assertEqual(last_doc["o"]["i"], 2)
        self.assertIsNone(self.opman.checkpoint)

        # Last checkpoint exists, no collection dump, something in oplog
        collection.insert_many([{"i": i + 500} for i in range(1000)])
        entry = list(self.primary_conn["local"]["oplog.rs"].find(skip=200, limit=-2))
        self.opman.update_checkpoint(entry[0]["ts"])
        cursor, cursor_empty = self.opman.init_cursor()
        self.assertEqual(next(cursor)["ts"], entry[1]["ts"])
        self.assertEqual(self.opman.checkpoint, entry[0]["ts"])
        self.assertEqual(self.opman.read_last_checkpoint(), entry[0]["ts"])

        # Last checkpoint is behind
        self.opman.update_checkpoint(bson.Timestamp(1, 0))
        cursor, cursor_empty = self.opman.init_cursor()
        self.assertTrue(cursor_empty)
        self.assertEqual(cursor, None)
        self.assertEqual(self.opman.checkpoint, bson.Timestamp(1, 0))

    def test_namespace_mapping(self):
        """Test mapping of namespaces
        Cases:

        upsert/delete/update of documents:
        1. in namespace set, mapping provided
        2. outside of namespace set, mapping provided
        """

        source_ns = ["test.test1", "test.test2"]
        phony_ns = ["test.phony1", "test.phony2"]
        dest_mapping = {
            "test.test1": "test.test1_dest",
            "test.test2": "test.test2_dest",
        }
        self.opman.namespace_config = NamespaceConfig(
            namespace_set=source_ns, namespace_options=dest_mapping
        )
        docman = self.opman.doc_managers[0]
        # start replicating
        self.opman.start()

        base_doc = {"_id": 1, "name": "superman"}

        # doc in namespace set
        for ns in source_ns:
            db, coll = ns.split(".", 1)

            # test insert
            self.primary_conn[db][coll].insert_one(base_doc)

            assert_soon(lambda: len(docman._search()) == 1)
            self.assertEqual(docman._search()[0]["ns"], dest_mapping[ns])
            bad = [d for d in docman._search() if d["ns"] == ns]
            self.assertEqual(len(bad), 0)

            # test update
            self.primary_conn[db][coll].update_one(
                {"_id": 1}, {"$set": {"weakness": "kryptonite"}}
            )

            def update_complete():
                docs = docman._search()
                for d in docs:
                    if d.get("weakness") == "kryptonite":
                        return True
                    return False

            assert_soon(update_complete)
            self.assertEqual(docman._search()[0]["ns"], dest_mapping[ns])
            bad = [d for d in docman._search() if d["ns"] == ns]
            self.assertEqual(len(bad), 0)

            # test delete
            self.primary_conn[db][coll].delete_one({"_id": 1})
            assert_soon(lambda: len(docman._search()) == 0)
            bad = [d for d in docman._search() if d["ns"] == dest_mapping[ns]]
            self.assertEqual(len(bad), 0)

            # cleanup
            self.primary_conn[db][coll].delete_many({})
            self.opman.doc_managers[0]._delete()

        # doc not in namespace set
        for ns in phony_ns:
            db, coll = ns.split(".", 1)

            # test insert
            self.primary_conn[db][coll].insert_one(base_doc)
            time.sleep(1)
            self.assertEqual(len(docman._search()), 0)
            # test update
            self.primary_conn[db][coll].update_one(
                {"_id": 1}, {"$set": {"weakness": "kryptonite"}}
            )
            time.sleep(1)
            self.assertEqual(len(docman._search()), 0)

    def test_many_targets(self):
        """Test that one OplogThread is capable of replicating to more than
        one target.
        """
        doc_managers = [DocManager(), DocManager(), DocManager()]
        self.opman.doc_managers = doc_managers

        # start replicating
        self.opman.start()
        self.primary_conn["test"]["test"].insert_one(
            {"name": "kermit", "color": "green"}
        )
        self.primary_conn["test"]["test"].insert_one(
            {"name": "elmo", "color": "firetruck red"}
        )

        assert_soon(
            lambda: sum(len(d._search()) for d in doc_managers) == 6,
            "OplogThread should be able to replicate to multiple targets",
        )

        self.primary_conn["test"]["test"].delete_one({"name": "elmo"})

        assert_soon(
            lambda: sum(len(d._search()) for d in doc_managers) == 3,
            "OplogThread should be able to replicate to multiple targets",
        )
        for d in doc_managers:
            self.assertEqual(d._search()[0]["name"], "kermit")

    def test_upgrade_oplog_progress(self):
        first_oplog_ts = self.opman.oplog.find_one()["ts"]
        # Old format oplog progress file:
        progress = {str(self.opman.oplog): bson_ts_to_long(first_oplog_ts)}
        # Set up oplog managers to use the old format.
        oplog_progress = LockingDict()
        oplog_progress.dict = progress
        self.opman.oplog_progress = oplog_progress
        # Cause the oplog managers to update their checkpoints.
        self.opman.update_checkpoint(first_oplog_ts)
        # New format should be in place now.
        new_format = {self.opman.replset_name: first_oplog_ts}
        self.assertEqual(new_format, self.opman.oplog_progress.get_dict())


if __name__ == "__main__":
    unittest.main()
