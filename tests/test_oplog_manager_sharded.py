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

import threading
import time
import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

from pymongo import MongoClient
import bson
import pymongo
from pymongo.read_preferences import ReadPreference

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.util import retry_until_ok
from tests import mongo_host
from tests.setup_cluster import (
    kill_mongo_proc,
    restart_mongo_proc,
    start_cluster,
    get_shard,
    kill_all
)
from tests.util import assert_soon


class TestOplogManagerSharded(unittest.TestCase):
    """Defines all test cases for OplogThreads running on a sharded
    cluster
    """

    def setUp(self):
        """ Initialize the cluster:

        Clean out the databases used by the tests
        Make connections to mongos, mongods
        Create and shard test collections
        Create OplogThreads
        """
        # Start the cluster with a mongos on port 27217
        self.mongos_p = start_cluster()

        # Connection to mongos
        mongos_address = '%s:%d' % (mongo_host, self.mongos_p)
        self.mongos_conn = MongoClient(mongos_address)

        # Connections to the shards
        shard1_ports = get_shard(self.mongos_p, 0)
        shard2_ports = get_shard(self.mongos_p, 1)
        self.shard1_prim_p = shard1_ports['primary']
        self.shard1_scnd_p = shard1_ports['secondaries'][0]
        self.shard2_prim_p = shard2_ports['primary']
        self.shard2_scnd_p = shard2_ports['secondaries'][0]
        self.shard1_conn = MongoClient('%s:%d'
                                       % (mongo_host, self.shard1_prim_p),
                                       replicaSet="demo-set-0")
        self.shard2_conn = MongoClient('%s:%d'
                                       % (mongo_host, self.shard2_prim_p),
                                       replicaSet="demo-set-1")
        self.shard1_secondary_conn = MongoClient(
            '%s:%d' % (mongo_host, self.shard1_scnd_p),
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        self.shard2_secondary_conn = MongoClient(
            '%s:%d' % (mongo_host, self.shard2_scnd_p),
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

        # Wipe any test data
        self.mongos_conn["test"]["mcsharded"].drop()

        # Create and shard the collection test.mcsharded on the "i" field
        self.mongos_conn["test"]["mcsharded"].ensure_index("i")
        self.mongos_conn.admin.command("enableSharding", "test")
        self.mongos_conn.admin.command("shardCollection",
                                       "test.mcsharded",
                                       key={"i": 1})

        # Pre-split the collection so that:
        # i < 1000            lives on shard1
        # i >= 1000           lives on shard2
        self.mongos_conn.admin.command(bson.SON([
            ("split", "test.mcsharded"),
            ("middle", {"i": 1000})
        ]))

        # disable the balancer
        self.mongos_conn.config.settings.update(
            {"_id": "balancer"},
            {"$set": {"stopped": True}},
            upsert=True
        )

        # Move chunks to their proper places
        try:
            self.mongos_conn["admin"].command(
                "moveChunk",
                "test.mcsharded",
                find={"i": 1},
                to="demo-set-0"
            )
        except pymongo.errors.OperationFailure:
            pass        # chunk may already be on the correct shard
        try:
            self.mongos_conn["admin"].command(
                "moveChunk",
                "test.mcsharded",
                find={"i": 1000},
                to="demo-set-1"
            )
        except pymongo.errors.OperationFailure:
            pass        # chunk may already be on the correct shard

        # Make sure chunks are distributed correctly
        self.mongos_conn["test"]["mcsharded"].insert({"i": 1})
        self.mongos_conn["test"]["mcsharded"].insert({"i": 1000})

        def chunks_moved():
            doc1 = self.shard1_conn.test.mcsharded.find_one()
            doc2 = self.shard2_conn.test.mcsharded.find_one()
            if None in (doc1, doc2):
                return False
            return doc1['i'] == 1 and doc2['i'] == 1000
        assert_soon(chunks_moved)
        self.mongos_conn.test.mcsharded.remove()

        # create a new oplog progress file
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()

        # Oplog threads (oplog manager) for each shard
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        self.opman1 = OplogThread(
            primary_conn=self.shard1_conn,
            main_address='%s:%d' % (mongo_host, self.mongos_p),
            oplog_coll=self.shard1_conn["local"]["oplog.rs"],
            is_sharded=True,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mcsharded", "test.mcunsharded"],
            auth_key=None,
            auth_username=None
        )
        self.opman2 = OplogThread(
            primary_conn=self.shard2_conn,
            main_address='%s:%d' % (mongo_host, self.mongos_p),
            oplog_coll=self.shard2_conn["local"]["oplog.rs"],
            is_sharded=True,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mcsharded", "test.mcunsharded"],
            auth_key=None,
            auth_username=None
        )

    def tearDown(self):
        try:
            self.opman1.join()
        except RuntimeError:
            pass                # thread may not have been started
        try:
            self.opman2.join()
        except RuntimeError:
            pass                # thread may not have been started
        self.mongos_conn.close()
        self.shard1_conn.close()
        self.shard2_conn.close()
        self.shard1_secondary_conn.close()
        self.shard2_secondary_conn.close()
        kill_all()

    def test_retrieve_doc(self):
        """ Test the retrieve_doc method """

        # Trivial case where the oplog entry is None
        self.assertEqual(self.opman1.retrieve_doc(None), None)

        # Retrieve a document from insert operation in oplog
        doc = {"name": "mango", "type": "fruit",
               "ns": "test.mcsharded", "weight": 3.24, "i": 1}
        self.mongos_conn["test"]["mcsharded"].insert(doc)
        oplog_entries = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.DESCENDING)],
            limit=1
        )
        oplog_entry = next(oplog_entries)
        self.assertEqual(self.opman1.retrieve_doc(oplog_entry), doc)

        # Retrieve a document from update operation in oplog
        self.mongos_conn["test"]["mcsharded"].update(
            {"i": 1},
            {"$set": {"sounds-like": "mongo"}}
        )
        oplog_entries = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.DESCENDING)],
            limit=1
        )
        doc["sounds-like"] = "mongo"
        self.assertEqual(self.opman1.retrieve_doc(next(oplog_entries)), doc)

        # Retrieve a document from remove operation in oplog
        # (expected: None)
        self.mongos_conn["test"]["mcsharded"].remove({
            "i": 1
        })
        oplog_entries = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.DESCENDING)],
            limit=1
        )
        self.assertEqual(self.opman1.retrieve_doc(next(oplog_entries)), None)

        # Retrieve a document with bad _id
        # (expected: None)
        oplog_entry["o"]["_id"] = "ThisIsNotAnId123456789"
        self.assertEqual(self.opman1.retrieve_doc(oplog_entry), None)

    def test_get_oplog_cursor(self):
        """Test the get_oplog_cursor method"""

        # Trivial case: timestamp is None
        self.assertEqual(self.opman1.get_oplog_cursor(None), None)

        # earliest entry is after given timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.mongos_conn["test"]["mcsharded"].insert(doc)
        self.assertEqual(self.opman1.get_oplog_cursor(
            bson.Timestamp(1, 0)), None)

        # earliest entry is the only one at/after timestamp
        latest_timestamp = self.opman1.get_last_oplog_timestamp()
        cursor = self.opman1.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(self.opman1.retrieve_doc(cursor[0]), doc)

        # many entries before and after timestamp
        for i in range(2, 2002):
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i
            })
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
        pivot1 = oplog1.skip(400).limit(1)[0]
        pivot2 = oplog2.skip(400).limit(1)[0]

        cursor1 = self.opman1.get_oplog_cursor(pivot1["ts"])
        cursor2 = self.opman2.get_oplog_cursor(pivot2["ts"])
        self.assertEqual(cursor1.count(), oplog1_count - 400)
        self.assertEqual(cursor2.count(), oplog2_count - 400)

        # get_oplog_cursor fast-forwards *one doc beyond* the given timestamp
        doc1 = self.mongos_conn["test"]["mcsharded"].find_one(
            {"_id": next(cursor1)["o"]["_id"]})
        doc2 = self.mongos_conn["test"]["mcsharded"].find_one(
            {"_id": next(cursor2)["o"]["_id"]})
        self.assertEqual(doc1["i"], self.opman1.retrieve_doc(pivot1)["i"] + 1)
        self.assertEqual(doc2["i"], self.opman2.retrieve_doc(pivot2)["i"] + 1)

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
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i + 500
            })
        oplog1 = self.shard1_conn["local"]["oplog.rs"]
        oplog1 = oplog1.find().sort("$natural", pymongo.DESCENDING).limit(1)[0]
        oplog2 = self.shard2_conn["local"]["oplog.rs"]
        oplog2 = oplog2.find().sort("$natural", pymongo.DESCENDING).limit(1)[0]
        self.assertEqual(self.opman1.get_last_oplog_timestamp(),
                         oplog1["ts"])
        self.assertEqual(self.opman2.get_last_oplog_timestamp(),
                         oplog2["ts"])

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
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i + 500
            })
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
        5. last checkpoint exists
        """

        # N.B. these sub-cases build off of each other and cannot be re-ordered
        # without side-effects

        # No last checkpoint, no collection dump, nothing in oplog
        # "change oplog collection" to put nothing in oplog
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        self.opman1.collection_dump = False
        self.opman2.collection_dump = False
        self.assertEqual(self.opman1.init_cursor(), None)
        self.assertEqual(self.opman1.checkpoint, None)
        self.assertEqual(self.opman2.init_cursor(), None)
        self.assertEqual(self.opman2.checkpoint, None)

        # No last checkpoint, empty collections, nothing in oplog
        self.opman1.collection_dump = True
        self.opman2.collection_dump = True
        self.assertEqual(self.opman1.init_cursor(), None)
        self.assertEqual(self.opman1.checkpoint, None)
        self.assertEqual(self.opman2.init_cursor(), None)
        self.assertEqual(self.opman2.checkpoint, None)

        # No last checkpoint, empty collections, something in oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard2_conn["local"]["oplog.rs"]
        oplog_startup_ts = self.opman2.get_last_oplog_timestamp()
        collection = self.mongos_conn["test"]["mcsharded"]
        collection.insert({"i": 1})
        collection.remove({"i": 1})
        time.sleep(3)
        last_ts1 = self.opman1.get_last_oplog_timestamp()
        self.assertEqual(next(self.opman1.init_cursor())["ts"], last_ts1)
        self.assertEqual(self.opman1.checkpoint, last_ts1)
        with self.opman1.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman1.oplog)], last_ts1)
        # init_cursor should point to startup message in shard2 oplog
        cursor = self.opman2.init_cursor()
        self.assertEqual(next(cursor)["ts"], oplog_startup_ts)
        self.assertEqual(self.opman2.checkpoint, oplog_startup_ts)

        # No last checkpoint, non-empty collections, stuff in oplog
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        collection.insert({"i": 1200})
        last_ts2 = self.opman2.get_last_oplog_timestamp()
        self.assertEqual(next(self.opman1.init_cursor())["ts"], last_ts1)
        self.assertEqual(self.opman1.checkpoint, last_ts1)
        with self.opman1.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman1.oplog)], last_ts1)
        self.assertEqual(next(self.opman2.init_cursor())["ts"], last_ts2)
        self.assertEqual(self.opman2.checkpoint, last_ts2)
        with self.opman2.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman2.oplog)], last_ts2)

        # Last checkpoint exists
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        for i in range(1000):
            collection.insert({"i": i + 500})
        entry1 = list(
            self.shard1_conn["local"]["oplog.rs"].find(skip=200, limit=2))
        entry2 = list(
            self.shard2_conn["local"]["oplog.rs"].find(skip=200, limit=2))
        progress.get_dict()[str(self.opman1.oplog)] = entry1[0]["ts"]
        progress.get_dict()[str(self.opman2.oplog)] = entry2[0]["ts"]
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        self.opman1.checkpoint = self.opman2.checkpoint = None
        cursor1 = self.opman1.init_cursor()
        cursor2 = self.opman2.init_cursor()
        self.assertEqual(entry1[1]["ts"], next(cursor1)["ts"])
        self.assertEqual(entry2[1]["ts"], next(cursor2)["ts"])
        self.assertEqual(self.opman1.checkpoint, entry1[0]["ts"])
        self.assertEqual(self.opman2.checkpoint, entry2[0]["ts"])
        with self.opman1.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman1.oplog)],
                             entry1[0]["ts"])
        with self.opman2.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman2.oplog)],
                             entry2[0]["ts"])

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
        db_main.insert({"i": 0}, w=2)
        db_main.insert({"i": 1000}, w=2)
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].count(), 1)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].count(), 1)

        # Case 1: only one primary goes down, shard1 in this case
        kill_mongo_proc(self.shard1_prim_p, destroy=False)

        # Wait for the secondary to be promoted
        shard1_secondary_admin = self.shard1_secondary_conn["admin"]
        assert_soon(
            lambda: shard1_secondary_admin.command("isMaster")["ismaster"])

        # Insert another document. This will be rolled back later
        retry_until_ok(db_main.insert, {"i": 1})
        db_secondary1 = self.shard1_secondary_conn["test"]["mcsharded"]
        db_secondary2 = self.shard2_secondary_conn["test"]["mcsharded"]
        self.assertEqual(db_secondary1.count(), 2)

        # Wait for replication on the doc manager
        # Note that both OplogThreads share the same doc manager
        c = lambda: len(self.opman1.doc_managers[0]._search()) == 3
        assert_soon(c, "not all writes were replicated to doc manager",
                    max_tries=120)

        # Kill the new primary
        kill_mongo_proc(self.shard1_scnd_p, destroy=False)

        # Start both servers back up
        restart_mongo_proc(self.shard1_prim_p)
        primary_admin = self.shard1_conn["admin"]
        c = lambda: primary_admin.command("isMaster")["ismaster"]
        assert_soon(lambda: retry_until_ok(c))
        restart_mongo_proc(self.shard1_scnd_p)
        secondary_admin = self.shard1_secondary_conn["admin"]
        c = lambda: secondary_admin.command("replSetGetStatus")["myState"] == 2
        assert_soon(c)
        query = {"i": {"$lt": 1000}}
        assert_soon(lambda: retry_until_ok(db_main.find(query).count) > 0)

        # Only first document should exist in MongoDB
        self.assertEqual(db_main.find(query).count(), 1)
        self.assertEqual(db_main.find_one(query)["i"], 0)

        # Same should hold for the doc manager
        docman_docs = [d for d in self.opman1.doc_managers[0]._search()
                       if d["i"] < 1000]
        self.assertEqual(len(docman_docs), 1)
        self.assertEqual(docman_docs[0]["i"], 0)

        # Wait for previous rollback to complete
        def rollback_done():
            secondary1_count = retry_until_ok(db_secondary1.count)
            secondary2_count = retry_until_ok(db_secondary2.count)
            return (1, 1) == (secondary1_count, secondary2_count)
        assert_soon(rollback_done,
                    "rollback never replicated to one or more secondaries")

        ##############################

        # Case 2: Primaries on both shards go down
        kill_mongo_proc(self.shard1_prim_p, destroy=False)
        kill_mongo_proc(self.shard2_prim_p, destroy=False)

        # Wait for the secondaries to be promoted
        shard1_secondary_admin = self.shard1_secondary_conn["admin"]
        shard2_secondary_admin = self.shard2_secondary_conn["admin"]
        assert_soon(
            lambda: shard1_secondary_admin.command("isMaster")["ismaster"])
        assert_soon(
            lambda: shard2_secondary_admin.command("isMaster")["ismaster"])

        # Insert another document on each shard. These will be rolled back later
        retry_until_ok(db_main.insert, {"i": 1})
        self.assertEqual(db_secondary1.count(), 2)
        retry_until_ok(db_main.insert, {"i": 1001})
        self.assertEqual(db_secondary2.count(), 2)

        # Wait for replication on the doc manager
        c = lambda: len(self.opman1.doc_managers[0]._search()) == 4
        assert_soon(c, "not all writes were replicated to doc manager")

        # Kill the new primaries
        kill_mongo_proc(self.shard1_scnd_p, destroy=False)
        kill_mongo_proc(self.shard2_scnd_p, destroy=False)

        # Start the servers back up...
        # Shard 1
        restart_mongo_proc(self.shard1_prim_p)
        c = lambda: self.shard1_conn['admin'].command("isMaster")["ismaster"]
        assert_soon(lambda: retry_until_ok(c))
        restart_mongo_proc(self.shard1_scnd_p)
        secondary_admin = self.shard1_secondary_conn["admin"]
        c = lambda: secondary_admin.command("replSetGetStatus")["myState"] == 2
        assert_soon(c)
        # Shard 2
        restart_mongo_proc(self.shard2_prim_p)
        c = lambda: self.shard2_conn['admin'].command("isMaster")["ismaster"]
        assert_soon(lambda: retry_until_ok(c))
        restart_mongo_proc(self.shard2_scnd_p)
        secondary_admin = self.shard2_secondary_conn["admin"]
        c = lambda: secondary_admin.command("replSetGetStatus")["myState"] == 2
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
        i_values = [d["i"] for d in self.opman1.doc_managers[0]._search()]
        self.assertEqual(len(i_values), 2)
        self.assertIn(0, i_values)
        self.assertIn(1000, i_values)

    def test_with_chunk_migration(self):
        """Test that DocManagers have proper state after both a successful
        and an unsuccessful chunk migration
        """

        # Start replicating to dummy doc managers
        self.opman1.start()
        self.opman2.start()

        collection = self.mongos_conn["test"]["mcsharded"]
        for i in range(1000):
            collection.insert({"i": i + 500})
        # Assert current state of the mongoverse
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].find().count(),
                         500)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].find().count(),
                         500)
        assert_soon(lambda: len(self.opman1.doc_managers[0]._search()) == 1000)

        # Test successful chunk move from shard 1 to shard 2
        self.mongos_conn["admin"].command(
            "moveChunk",
            "test.mcsharded",
            find={"i": 1},
            to="demo-set-1"
        )

        # doc manager should still have all docs
        all_docs = self.opman1.doc_managers[0]._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

        # Mark the collection as "dropped". This will cause migration to fail.
        self.mongos_conn["config"]["collections"].update(
            {"_id": "test.mcsharded"},
            {"$set": {"dropped": True}}
        )

        # Test unsuccessful chunk move from shard 2 to shard 1
        def fail_to_move_chunk():
            self.mongos_conn["admin"].command(
                "moveChunk",
                "test.mcsharded",
                find={"i": 1},
                to="demo-set-0"
            )
        self.assertRaises(pymongo.errors.OperationFailure, fail_to_move_chunk)
        # doc manager should still have all docs
        all_docs = self.opman1.doc_managers[0]._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

    def test_with_orphan_documents(self):
        """Test that DocManagers have proper state after a chunk migration
        that resuts in orphaned documents.
        """
        # Start replicating to dummy doc managers
        self.opman1.start()
        self.opman2.start()

        collection = self.mongos_conn["test"]["mcsharded"]
        collection.insert({"i": i + 500} for i in range(1000))
        # Assert current state of the mongoverse
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].find().count(),
                         500)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].find().count(),
                         500)
        assert_soon(lambda: len(self.opman1.doc_managers[0]._search()) == 1000)

        # Stop replication using the 'rsSyncApplyStop' failpoint
        self.shard1_conn.admin.command(
            "configureFailPoint", "rsSyncApplyStop",
            mode="alwaysOn"
        )

        # Move a chunk from shard2 to shard1
        def move_chunk():
            try:
                self.mongos_conn["admin"].command(
                    "moveChunk",
                    "test.mcsharded",
                    find={"i": 1000},
                    to="demo-set-0"
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
        self.assertNotEqual(opid, None, "could not find moveChunk operation")
        # Kill moveChunk with the opid
        self.mongos_conn["test"]["$cmd.sys.killop"].find_one({"op": opid})

        # Mongo Connector should not become confused by unsuccessful chunk move
        docs = self.opman1.doc_managers[0]._search()
        self.assertEqual(len(docs), 1000)
        self.assertEqual(sorted(d["i"] for d in docs),
                         list(range(500, 1500)))

        # cleanup
        mover.join()

if __name__ == '__main__':
    unittest.main()
