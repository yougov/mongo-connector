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

import os
import sys
import threading
import time

import bson
import pymongo
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

sys.path[0:0] = [""]  # noqa

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.test_utils import (
    assert_soon,
    close_client,
    ShardedCluster,
    ShardedClusterSingle,
)
from mongo_connector.util import retry_until_ok, bson_ts_to_long
from tests import unittest, SkipTest


class ShardedClusterTestCase(unittest.TestCase):
    def set_up_sharded_cluster(self, sharded_cluster_type):
        """ Initialize the cluster:

        Clean out the databases used by the tests
        Make connections to mongos, mongods
        Create and shard test collections
        Create OplogThreads
        """
        self.cluster = sharded_cluster_type().start()

        # Connection to mongos
        self.mongos_conn = self.cluster.client()

        # Connections to the shards
        self.shard1_conn = self.cluster.shards[0].client()
        self.shard2_conn = self.cluster.shards[1].client()

        # Wipe any test data
        self.mongos_conn["test"]["mcsharded"].drop()

        # Disable the balancer before creating the collection
        self.mongos_conn.config.settings.update_one(
            {"_id": "balancer"}, {"$set": {"stopped": True}}, upsert=True
        )

        # Create and shard the collection test.mcsharded on the "i" field
        self.mongos_conn["test"]["mcsharded"].create_index("i")
        self.mongos_conn.admin.command("enableSharding", "test")
        self.mongos_conn.admin.command(
            "shardCollection", "test.mcsharded", key={"i": 1}
        )

        # Pre-split the collection so that:
        # i < 1000            lives on shard1
        # i >= 1000           lives on shard2
        self.mongos_conn.admin.command(
            bson.SON([("split", "test.mcsharded"), ("middle", {"i": 1000})])
        )

        # Move chunks to their proper places
        try:
            self.mongos_conn["admin"].command(
                "moveChunk", "test.mcsharded", find={"i": 1}, to="demo-set-0"
            )
        except pymongo.errors.OperationFailure:
            pass
        try:
            self.mongos_conn["admin"].command(
                "moveChunk", "test.mcsharded", find={"i": 1000}, to="demo-set-1"
            )
        except pymongo.errors.OperationFailure:
            pass

        # Make sure chunks are distributed correctly
        self.mongos_conn["test"]["mcsharded"].insert_one({"i": 1})
        self.mongos_conn["test"]["mcsharded"].insert_one({"i": 1000})

        def chunks_moved():
            doc1 = self.shard1_conn.test.mcsharded.find_one()
            doc2 = self.shard2_conn.test.mcsharded.find_one()
            if None in (doc1, doc2):
                return False
            return doc1["i"] == 1 and doc2["i"] == 1000

        assert_soon(
            chunks_moved,
            max_tries=120,
            message="chunks not moved? doc1=%r, doc2=%r"
            % (
                self.shard1_conn.test.mcsharded.find_one(),
                self.shard2_conn.test.mcsharded.find_one(),
            ),
        )
        self.mongos_conn.test.mcsharded.delete_many({})

        # create a new oplog progress file
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        open("oplog.timestamp", "w").close()

        # Oplog threads (oplog manager) for each shard
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        namespace_config = NamespaceConfig(
            namespace_set=["test.mcsharded", "test.mcunsharded"]
        )
        self.opman1 = OplogThread(
            primary_client=self.shard1_conn,
            doc_managers=(doc_manager,),
            oplog_progress_dict=oplog_progress,
            namespace_config=namespace_config,
            mongos_client=self.mongos_conn,
        )
        self.opman2 = OplogThread(
            primary_client=self.shard2_conn,
            doc_managers=(doc_manager,),
            oplog_progress_dict=oplog_progress,
            namespace_config=namespace_config,
            mongos_client=self.mongos_conn,
        )

    def tearDown(self):
        try:
            self.opman1.join()
        except RuntimeError:
            pass  # thread may not have been started
        try:
            self.opman2.join()
        except RuntimeError:
            pass  # thread may not have been started
        close_client(self.mongos_conn)
        close_client(self.shard1_conn)
        close_client(self.shard2_conn)
        self.cluster.stop()


class TestOplogManagerShardedSingle(ShardedClusterTestCase):
    """Defines all test cases for OplogThreads running on a sharded
    cluster with single node replica sets.
    """

    def setUp(self):
        self.set_up_sharded_cluster(ShardedClusterSingle)

    def test_get_oplog_cursor(self):
        """Test the get_oplog_cursor method"""

        # timestamp = None
        cursor1 = self.opman1.get_oplog_cursor(None)
        oplog1 = self.shard1_conn["local"]["oplog.rs"].find({"op": {"$ne": "n"}})
        self.assertEqual(list(cursor1), list(oplog1))

        cursor2 = self.opman2.get_oplog_cursor(None)
        oplog2 = self.shard2_conn["local"]["oplog.rs"].find({"op": {"$ne": "n"}})
        self.assertEqual(list(cursor2), list(oplog2))

        # earliest entry is the only one at/after timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.mongos_conn["test"]["mcsharded"].insert_one(doc)
        latest_timestamp = self.opman1.get_last_oplog_timestamp()
        cursor = self.opman1.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        entries = list(cursor)
        self.assertEqual(len(entries), 1)
        next_entry_id = entries[0]["o"]["_id"]
        retrieved = self.mongos_conn.test.mcsharded.find_one(next_entry_id)
        self.assertEqual(retrieved, doc)

        # many entries before and after timestamp
        for i in range(2, 2002):
            self.mongos_conn["test"]["mcsharded"].insert_one({"i": i})
        oplog1 = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.ASCENDING)]
        )
        oplog2 = self.shard2_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.ASCENDING)]
        )

        # oplogs should have records for inserts performed, plus
        # various other messages
        oplog1_count = oplog1.count()
        oplog2_count = oplog2.count()
        self.assertGreaterEqual(oplog1_count, 998)
        self.assertGreaterEqual(oplog2_count, 1002)
        pivot1 = oplog1.skip(400).limit(-1)[0]
        pivot2 = oplog2.skip(400).limit(-1)[0]

        cursor1 = self.opman1.get_oplog_cursor(pivot1["ts"])
        cursor2 = self.opman2.get_oplog_cursor(pivot2["ts"])
        self.assertEqual(cursor1.count(), oplog1_count - 400)
        self.assertEqual(cursor2.count(), oplog2_count - 400)

    def test_get_last_oplog_timestamp(self):
        """Test the get_last_oplog_timestamp method"""

        # "empty" the oplog
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        self.assertEqual(self.opman1.get_last_oplog_timestamp(), None)
        self.assertEqual(self.opman2.get_last_oplog_timestamp(), None)

        # Test non-empty oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard2_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.mongos_conn["test"]["mcsharded"].insert_one({"i": i + 500})
        oplog1 = self.shard1_conn["local"]["oplog.rs"]
        oplog1 = oplog1.find().sort("$natural", pymongo.DESCENDING).limit(-1)[0]
        oplog2 = self.shard2_conn["local"]["oplog.rs"]
        oplog2 = oplog2.find().sort("$natural", pymongo.DESCENDING).limit(-1)[0]
        self.assertEqual(self.opman1.get_last_oplog_timestamp(), oplog1["ts"])
        self.assertEqual(self.opman2.get_last_oplog_timestamp(), oplog2["ts"])

    def test_dump_collection(self):
        """Test the dump_collection method

        Cases:

        1. empty oplog
        2. non-empty oplog
        """

        # Test with empty oplog
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        last_ts1 = self.opman1.dump_collection()
        last_ts2 = self.opman2.dump_collection()
        self.assertEqual(last_ts1, None)
        self.assertEqual(last_ts2, None)

        # Test with non-empty oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard2_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.mongos_conn["test"]["mcsharded"].insert_one({"i": i + 500})
        last_ts1 = self.opman1.get_last_oplog_timestamp()
        last_ts2 = self.opman2.get_last_oplog_timestamp()
        self.assertEqual(last_ts1, self.opman1.dump_collection())
        self.assertEqual(last_ts2, self.opman2.dump_collection())
        self.assertEqual(len(self.opman1.doc_managers[0]._search()), 1000)

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
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        self.opman1.collection_dump = False
        self.opman2.collection_dump = False
        self.assertTrue(all(doc["op"] == "n" for doc in self.opman1.init_cursor()[0]))
        self.assertEqual(self.opman1.checkpoint, None)
        self.assertTrue(all(doc["op"] == "n" for doc in self.opman2.init_cursor()[0]))
        self.assertEqual(self.opman2.checkpoint, None)

        # No last checkpoint, empty collections, nothing in oplog
        self.opman1.collection_dump = self.opman2.collection_dump = True

        cursor, cursor_empty = self.opman1.init_cursor()
        self.assertEqual(cursor, None)
        self.assertTrue(cursor_empty)
        self.assertEqual(self.opman1.checkpoint, None)
        cursor, cursor_empty = self.opman2.init_cursor()
        self.assertEqual(cursor, None)
        self.assertTrue(cursor_empty)
        self.assertEqual(self.opman2.checkpoint, None)

        # No last checkpoint, empty collections, something in oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard2_conn["local"]["oplog.rs"]
        oplog_startup_ts = self.opman2.get_last_oplog_timestamp()
        collection = self.mongos_conn["test"]["mcsharded"]
        collection.insert_one({"i": 1})
        collection.delete_one({"i": 1})
        time.sleep(3)
        last_ts1 = self.opman1.get_last_oplog_timestamp()
        cursor, cursor_empty = self.opman1.init_cursor()
        self.assertFalse(cursor_empty)
        self.assertEqual(self.opman1.checkpoint, last_ts1)
        self.assertEqual(self.opman1.read_last_checkpoint(), last_ts1)
        # init_cursor should point to startup message in shard2 oplog
        cursor, cursor_empty = self.opman2.init_cursor()
        self.assertFalse(cursor_empty)
        self.assertEqual(self.opman2.checkpoint, oplog_startup_ts)

        # No last checkpoint, no collection dump, stuff in oplog
        # If collection dump is false the checkpoint should not be set
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        self.opman1.collection_dump = self.opman2.collection_dump = False
        self.opman1.checkpoint = self.opman2.checkpoint = None
        collection.insert_one({"i": 1200})
        cursor, cursor_empty = self.opman1.init_cursor()
        self.assertFalse(cursor_empty)
        self.assertIsNone(self.opman1.checkpoint)
        self.assertEqual(next(cursor), next(self.opman1.get_oplog_cursor()))
        cursor, cursor_empty = self.opman2.init_cursor()
        self.assertFalse(cursor_empty)
        self.assertIsNone(self.opman2.checkpoint)
        for doc in cursor:
            last_doc = doc
        self.assertEqual(last_doc["o"]["i"], 1200)

        # Last checkpoint exists
        collection.insert_many([{"i": i + 500} for i in range(1000)])
        entry1 = list(self.shard1_conn["local"]["oplog.rs"].find(skip=200, limit=-2))
        entry2 = list(self.shard2_conn["local"]["oplog.rs"].find(skip=200, limit=-2))
        self.opman1.update_checkpoint(entry1[0]["ts"])
        self.opman2.update_checkpoint(entry2[0]["ts"])
        self.opman1.checkpoint = self.opman2.checkpoint = None
        cursor1, _ = self.opman1.init_cursor()
        cursor2, _ = self.opman2.init_cursor()
        self.assertEqual(entry1[1]["ts"], next(cursor1)["ts"])
        self.assertEqual(entry2[1]["ts"], next(cursor2)["ts"])
        self.assertEqual(self.opman1.checkpoint, entry1[0]["ts"])
        self.assertEqual(self.opman2.checkpoint, entry2[0]["ts"])
        self.assertEqual(self.opman1.read_last_checkpoint(), entry1[0]["ts"])
        self.assertEqual(self.opman2.read_last_checkpoint(), entry2[0]["ts"])

        # Last checkpoint is behind
        self.opman1.update_checkpoint(bson.Timestamp(1, 0))
        self.opman2.update_checkpoint(bson.Timestamp(1, 0))
        cursor, cursor_empty = self.opman1.init_cursor()
        self.assertTrue(cursor_empty)
        self.assertEqual(cursor, None)
        self.assertIsNotNone(self.opman1.checkpoint)
        cursor, cursor_empty = self.opman2.init_cursor()
        self.assertTrue(cursor_empty)
        self.assertEqual(cursor, None)
        self.assertIsNotNone(self.opman2.checkpoint)

    def test_with_chunk_migration(self):
        """Test that DocManagers have proper state after both a successful
        and an unsuccessful chunk migration
        """

        # Start replicating to dummy doc managers
        self.opman1.start()
        self.opman2.start()

        collection = self.mongos_conn["test"]["mcsharded"]
        for i in range(1000):
            collection.insert_one({"i": i + 500})
        # Assert current state of the mongoverse
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].find().count(), 500)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].find().count(), 500)
        assert_soon(lambda: len(self.opman1.doc_managers[0]._search()) == 1000)

        # Test successful chunk move from shard 1 to shard 2
        self.mongos_conn["admin"].command(
            "moveChunk", "test.mcsharded", find={"i": 1}, to="demo-set-1"
        )

        # doc manager should still have all docs
        all_docs = self.opman1.doc_managers[0]._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

        # Mark the collection as "dropped". This will cause migration to fail.
        self.mongos_conn["config"]["collections"].update_one(
            {"_id": "test.mcsharded"}, {"$set": {"dropped": True}}
        )

        # Test unsuccessful chunk move from shard 2 to shard 1
        def fail_to_move_chunk():
            self.mongos_conn["admin"].command(
                "moveChunk", "test.mcsharded", find={"i": 1}, to="demo-set-0"
            )

        self.assertRaises(pymongo.errors.OperationFailure, fail_to_move_chunk)
        # doc manager should still have all docs
        all_docs = self.opman1.doc_managers[0]._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

    def test_upgrade_oplog_progress(self):
        first_oplog_ts1 = self.opman1.oplog.find_one()["ts"]
        first_oplog_ts2 = self.opman2.oplog.find_one()["ts"]
        # Old format oplog progress file:
        progress = {
            str(self.opman1.oplog): bson_ts_to_long(first_oplog_ts1),
            str(self.opman2.oplog): bson_ts_to_long(first_oplog_ts2),
        }
        # Set up oplog managers to use the old format.
        oplog_progress = LockingDict()
        oplog_progress.dict = progress
        self.opman1.oplog_progress = oplog_progress
        self.opman2.oplog_progress = oplog_progress
        # Cause the oplog managers to update their checkpoints.
        self.opman1.update_checkpoint(first_oplog_ts1)
        self.opman2.update_checkpoint(first_oplog_ts2)
        # New format should be in place now.
        new_format = {
            self.opman1.replset_name: first_oplog_ts1,
            self.opman2.replset_name: first_oplog_ts2,
        }
        self.assertEqual(new_format, self.opman1.oplog_progress.get_dict())
        self.assertEqual(new_format, self.opman2.oplog_progress.get_dict())


class TestOplogManagerSharded(ShardedClusterTestCase):
    """Defines all test cases for OplogThreads running on a sharded
    cluster with three node replica sets.
    """

    def setUp(self):
        self.set_up_sharded_cluster(ShardedCluster)
        self.shard1_secondary_conn = self.cluster.shards[0].secondary.client(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        self.shard2_secondary_conn = self.cluster.shards[1].secondary.client(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

    def tearDown(self):
        super(TestOplogManagerSharded, self).tearDown()
        close_client(self.shard1_secondary_conn)
        close_client(self.shard2_secondary_conn)

    def test_with_orphan_documents(self):
        """Test that DocManagers have proper state after a chunk migration
        that resuts in orphaned documents.
        """
        # Start replicating to dummy doc managers
        self.opman1.start()
        self.opman2.start()

        collection = self.mongos_conn["test"]["mcsharded"]
        collection.insert_many([{"i": i + 500} for i in range(1000)])
        # Assert current state of the mongoverse
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].find().count(), 500)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].find().count(), 500)
        assert_soon(lambda: len(self.opman1.doc_managers[0]._search()) == 1000)

        # Stop replication using the 'rsSyncApplyStop' failpoint.
        # Note: this requires secondaries to ensure the subsequent moveChunk
        # command does not complete.
        self.shard1_conn.admin.command(
            "configureFailPoint", "rsSyncApplyStop", mode="alwaysOn"
        )

        # Move a chunk from shard2 to shard1
        def move_chunk():
            try:
                self.mongos_conn["admin"].command(
                    "moveChunk", "test.mcsharded", find={"i": 1000}, to="demo-set-0"
                )
            except pymongo.errors.OperationFailure:
                pass

        # moveChunk will never complete, so use another thread to continue
        mover = threading.Thread(target=move_chunk)
        mover.start()

        # wait for documents to start moving to shard 1
        assert_soon(lambda: self.shard1_conn.test.mcsharded.count() > 500)

        # Get opid for moveChunk command
        operations = self.mongos_conn.test.current_op()
        opid = None
        for op in operations["inprog"]:
            if op.get("query", {}).get("moveChunk"):
                opid = op["opid"]

        if opid is None:
            raise SkipTest(
                "could not find moveChunk operation, cannot test " "failed moveChunk"
            )
        # Kill moveChunk with the opid

        if self.mongos_conn.server_info()["versionArray"][:3] >= [3, 1, 2]:
            self.mongos_conn.admin.command("killOp", op=opid)
        else:
            self.mongos_conn["test"]["$cmd.sys.killop"].find_one({"op": opid})

        # Mongo Connector should not become confused by unsuccessful chunk move
        docs = self.opman1.doc_managers[0]._search()
        self.assertEqual(len(docs), 1000)
        self.assertEqual(sorted(d["i"] for d in docs), list(range(500, 1500)))
        self.shard1_conn.admin.command(
            "configureFailPoint", "rsSyncApplyStop", mode="off"
        )
        # cleanup
        mover.join()

    def test_rollback(self):
        """Test the rollback method in a sharded environment

        Cases:
        1. Documents on both shards, rollback on one shard
        2. Documents on both shards, rollback on both shards

        """

        self.opman1.start()
        self.opman2.start()

        # Insert first documents while primaries are up
        db_main = self.mongos_conn["test"]["mcsharded"]
        db_main2 = db_main.with_options(write_concern=WriteConcern(w=2))
        db_main2.insert_one({"i": 0})
        db_main2.insert_one({"i": 1000})
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].count(), 1)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].count(), 1)

        # Case 1: only one primary goes down, shard1 in this case
        self.cluster.shards[0].primary.stop(destroy=False)

        # Wait for the secondary to be promoted
        shard1_secondary_admin = self.shard1_secondary_conn["admin"]
        assert_soon(lambda: shard1_secondary_admin.command("isMaster")["ismaster"])

        # Insert another document. This will be rolled back later
        def insert_one(doc):
            if not db_main.find_one(doc):
                return db_main.insert_one(doc)
            return True

        assert_soon(
            lambda: retry_until_ok(insert_one, {"i": 1}),
            "could not insert into shard1 with one node down",
        )
        db_secondary1 = self.shard1_secondary_conn["test"]["mcsharded"]
        db_secondary2 = self.shard2_secondary_conn["test"]["mcsharded"]
        self.assertEqual(db_secondary1.count(), 2)

        # Wait for replication on the doc manager
        # Note that both OplogThreads share the same doc manager
        def c():
            return len(self.opman1.doc_managers[0]._search()) == 3

        assert_soon(c, "not all writes were replicated to doc manager", max_tries=120)

        # Kill the new primary
        self.cluster.shards[0].secondary.stop(destroy=False)

        # Start both servers back up
        self.cluster.shards[0].primary.start()
        primary_admin = self.shard1_conn["admin"]

        def c():
            return primary_admin.command("isMaster")["ismaster"]

        assert_soon(lambda: retry_until_ok(c))
        self.cluster.shards[0].secondary.start()
        secondary_admin = self.shard1_secondary_conn["admin"]

        def c():
            return secondary_admin.command("replSetGetStatus")["myState"] == 2

        assert_soon(c)
        query = {"i": {"$lt": 1000}}
        assert_soon(lambda: retry_until_ok(db_main.find(query).count) > 0)

        # Only first document should exist in MongoDB
        self.assertEqual(db_main.find(query).count(), 1)
        self.assertEqual(db_main.find_one(query)["i"], 0)

        def check_docman_rollback():
            docman_docs = [
                d for d in self.opman1.doc_managers[0]._search() if d["i"] < 1000
            ]
            return len(docman_docs) == 1 and docman_docs[0]["i"] == 0

        assert_soon(check_docman_rollback, "doc manager did not roll back")

        # Wait for previous rollback to complete.
        # Insert/delete one document to jump-start replication to secondaries
        # in MongoDB 3.x.
        db_main.insert_one({"i": -1})
        db_main.delete_one({"i": -1})

        def rollback_done():
            secondary1_count = retry_until_ok(db_secondary1.count)
            secondary2_count = retry_until_ok(db_secondary2.count)
            return (1, 1) == (secondary1_count, secondary2_count)

        assert_soon(
            rollback_done, "rollback never replicated to one or more secondaries"
        )

        ##############################

        # Case 2: Primaries on both shards go down
        self.cluster.shards[0].primary.stop(destroy=False)
        self.cluster.shards[1].primary.stop(destroy=False)

        # Wait for the secondaries to be promoted
        shard1_secondary_admin = self.shard1_secondary_conn["admin"]
        shard2_secondary_admin = self.shard2_secondary_conn["admin"]
        assert_soon(lambda: shard1_secondary_admin.command("isMaster")["ismaster"])
        assert_soon(lambda: shard2_secondary_admin.command("isMaster")["ismaster"])

        # Insert another document on each shard which will be rolled back later
        assert_soon(
            lambda: retry_until_ok(insert_one, {"i": 1}),
            "could not insert into shard1 with one node down",
        )
        self.assertEqual(db_secondary1.count(), 2)
        assert_soon(
            lambda: retry_until_ok(insert_one, {"i": 1001}),
            "could not insert into shard2 with one node down",
        )
        self.assertEqual(db_secondary2.count(), 2)

        # Wait for replication on the doc manager

        def c():
            return len(self.opman1.doc_managers[0]._search()) == 4

        assert_soon(c, "not all writes were replicated to doc manager")

        # Kill the new primaries
        self.cluster.shards[0].secondary.stop(destroy=False)
        self.cluster.shards[1].secondary.stop(destroy=False)

        # Start the servers back up...
        # Shard 1
        self.cluster.shards[0].primary.start()

        def c():
            return self.shard1_conn["admin"].command("isMaster")["ismaster"]

        assert_soon(lambda: retry_until_ok(c))
        self.cluster.shards[0].secondary.start()
        secondary_admin = self.shard1_secondary_conn["admin"]

        def c():
            return secondary_admin.command("replSetGetStatus")["myState"] == 2

        assert_soon(c)
        # Shard 2
        self.cluster.shards[1].primary.start()

        def c():
            return self.shard2_conn["admin"].command("isMaster")["ismaster"]

        assert_soon(lambda: retry_until_ok(c))
        self.cluster.shards[1].secondary.start()
        secondary_admin = self.shard2_secondary_conn["admin"]

        def c():
            return secondary_admin.command("replSetGetStatus")["myState"] == 2

        assert_soon(c)

        # Wait for the shards to come online
        assert_soon(lambda: retry_until_ok(db_main.find(query).count) > 0)
        query2 = {"i": {"$gte": 1000}}
        assert_soon(lambda: retry_until_ok(db_main.find(query2).count) > 0)

        # Only first documents should exist in MongoDB
        self.assertEqual(db_main.find(query).count(), 1)
        self.assertEqual(db_main.find_one(query)["i"], 0)
        self.assertEqual(db_main.find(query2).count(), 1)
        self.assertEqual(db_main.find_one(query2)["i"], 1000)

        # Same should hold for the doc manager
        assert_soon(lambda: len(self.opman1.doc_managers[0]._search()) == 2)
        i_values = [d["i"] for d in self.opman1.doc_managers[0]._search()]
        self.assertIn(0, i_values)
        self.assertIn(1000, i_values)


if __name__ == "__main__":
    unittest.main()
