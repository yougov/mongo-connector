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

import time
import os
import shutil
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

import bson
import pymongo
from pymongo.read_preferences import ReadPreference
try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.util import (bson_ts_to_long,
                                  retry_until_ok)
from tests.setup_cluster import (
    kill_mongo_proc,
    start_mongo_proc,
    start_cluster,
    kill_all,
    DEMO_SERVER_DATA,
    PORTS_ONE,
    PORTS_TWO
)
from tests.util import wait_for


class TestOplogManagerSharded(unittest.TestCase):
    """Defines all test cases for OplogThreads running on a sharded
    cluster
    """

    @classmethod
    def setUpClass(cls):
        """ Initialize the cluster:

        Clean out the databases used by the tests
        Make connections to mongos, mongods
        Create and shard test collections
        Create OplogThreads
        """
        # Create a new oplog progress file
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()

        # Start the cluster with a mongos on port 27217
        start_cluster(sharded=True)

        # Connection to mongos
        mongos_address = "localhost:%s" % PORTS_ONE["MONGOS"]
        cls.mongos_conn = Connection(mongos_address)

        # Connections to the shards
        cls.shard1_conn = Connection("localhost:%s" % PORTS_ONE["PRIMARY"])
        cls.shard2_conn = Connection("localhost:%s" % PORTS_TWO["PRIMARY"])
        cls.shard1_secondary_conn = Connection(
            "localhost:%s" % PORTS_ONE["SECONDARY"],
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        cls.shard2_secondary_conn = Connection(
            "localhost:%s" % PORTS_TWO["SECONDARY"],
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

        # Wipe any test data
        cls.mongos_conn["test"]["mcsharded"].drop()

        # Create and shard the collection test.mcsharded on the "i" field
        cls.mongos_conn["test"]["mcsharded"].ensure_index("i")
        cls.mongos_conn.admin.command("enableSharding", "test")
        cls.mongos_conn.admin.command("shardCollection",
                                      "test.mcsharded",
                                      key={"i": 1})
        # Pre-split the collection so that:
        # i < 1000            lives on shard1
        # i >= 1000           lives on shard2
        cls.mongos_conn.admin.command(bson.SON([
            ("split", "test.mcsharded"),
            ("middle", {"i": 1000})
        ]))
        # Tag shards/ranges to enforce chunk split rules
        cls.mongos_conn.config.shards.update(
            {"_id": "demo-repl"},
            {"$addToSet": {"tags": "small-i"}}
        )
        cls.mongos_conn.config.shards.update(
            {"_id": "demo-repl-2"},
            {"$addToSet": {"tags": "large-i"}}
        )
        cls.mongos_conn.config.tags.update(
            {"_id": bson.son.SON([("ns", "test.mcsharded"),
                                  ("min", {"i": bson.min_key.MinKey()})])},
            bson.son.SON([
                ("_id", bson.son.SON([
                        ("ns", "test.mcsharded"),
                        ("min", {"i": bson.min_key.MinKey()})
                    ])),
                ("ns", "test.mcsharded"),
                ("min", {"i": bson.min_key.MinKey()}),
                ("max", {"i": 1000}),
                ("tag", "small-i")
            ]),
            upsert=True
        )
        cls.mongos_conn.config.tags.update(
            {"_id": bson.son.SON([("ns", "test.mcsharded"),
                                  ("min", {"i": 1000})])},
            bson.son.SON([
                ("_id", bson.son.SON([
                        ("ns", "test.mcsharded"),
                        ("min", {"i": 1000})
                    ])),
                ("ns", "test.mcsharded"),
                ("min", {"i": 1000}),
                ("max", {"i": bson.max_key.MaxKey()}),
                ("tag", "large-i")
            ]),
            upsert=True
        )
        # Move chunks to their proper places
        try:
            cls.mongos_conn["admin"].command(
                bson.son.SON([
                    ("moveChunk", "test.mcsharded"),
                    ("find", {"i": 1}),
                    ("to", "demo-repl")
                ])
            )
        except pymongo.errors.OperationFailure:
            pass        # chunk may already be on the correct shard
        try:
            cls.mongos_conn["admin"].command(
                bson.son.SON([
                    ("moveChunk", "test.mcsharded"),
                    ("find", {"i": 1000}),
                    ("to", "demo-repl-2")
                ])
            )
        except pymongo.errors.OperationFailure:
            pass        # chunk may already be on the correct shard

        # Wait for things to settle down
        cls.mongos_conn["test"]["mcsharded"].insert({"i": 1})
        cls.mongos_conn["test"]["mcsharded"].insert({"i": 1000})

        def chunks_moved():
            shard1_done = cls.shard1_conn.test.mcsharded.find_one() is not None
            shard2_done = cls.shard2_conn.test.mcsharded.find_one() is not None
            return shard1_done and shard2_done
        assert(wait_for(chunks_moved))
        cls.mongos_conn.test.mcsharded.remove()

        # Oplog threads (oplog manager) for each shard
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        cls.opman1 = OplogThread(
            primary_conn=cls.shard1_conn,
            main_address=mongos_address,
            oplog_coll=cls.shard1_conn["local"]["oplog.rs"],
            is_sharded=True,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mcsharded", "test.mcunsharded"],
            auth_key=None,
            auth_username=None
        )
        cls.opman2 = OplogThread(
            primary_conn=cls.shard2_conn,
            main_address=mongos_address,
            oplog_coll=cls.shard2_conn["local"]["oplog.rs"],
            is_sharded=True,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mcsharded", "test.mcunsharded"],
            auth_key=None,
            auth_username=None
        )

    @classmethod
    def tearDownClass(cls):
        """ Kill the cluster
        """
        kill_all()

    def setUp(self):
        # clear oplog
        self.shard1_conn["local"]["oplog.rs"].drop()
        self.shard2_conn["local"]["oplog.rs"].drop()
        self.shard1_conn["local"].create_collection(
            "oplog.rs",
            size=1024 * 1024 * 100,       # 100MB
            capped=True
        )
        self.shard2_conn["local"].create_collection(
            "oplog.rs",
            size=1024 * 1024 * 100,       # 100MB
            capped=True
        )
        # re-sync secondaries
        try:
            self.shard1_secondary_conn["admin"].command("shutdown")
        except pymongo.errors.AutoReconnect:
            pass
        try:
            self.shard2_secondary_conn["admin"].command("shutdown")
        except pymongo.errors.AutoReconnect:
            pass
        data1 = os.path.join(DEMO_SERVER_DATA, "replset1b")
        data2 = os.path.join(DEMO_SERVER_DATA, "replset2b")
        shutil.rmtree(data1)
        shutil.rmtree(data2)
        os.mkdir(data1)
        os.mkdir(data2)
        conf1 = self.shard1_conn["local"]["system.replset"].find_one()
        conf2 = self.shard2_conn["local"]["system.replset"].find_one()
        conf1["version"] += 1
        conf2["version"] += 1
        self.shard1_conn["admin"].command({"replSetReconfig": conf1})
        self.shard2_conn["admin"].command({"replSetReconfig": conf2})
        start_mongo_proc(PORTS_ONE["SECONDARY"], "demo-repl", "replset1b",
                         "replset1b.log", None)
        start_mongo_proc(PORTS_TWO["SECONDARY"], "demo-repl-2", "replset2b",
                         "replset2b.log", None)

        def secondary_up(connection):
            def wrap():
                return retry_until_ok(
                    connection["admin"].command,
                    "replSetGetStatus"
                )["myState"] == 2
            return wrap
        wait_for(secondary_up(self.shard1_secondary_conn))
        wait_for(secondary_up(self.shard2_secondary_conn))

    def tearDown(self):
        self.mongos_conn["test"]["mcsharded"].remove()
        self.mongos_conn["test"]["mcunsharded"].remove()

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

        # startup message + insert at beginning of tests + many inserts
        self.assertEqual(oplog1.count(), 1 + 1 + 998)
        self.assertEqual(oplog2.count(), 1 + 1002)
        pivot1 = oplog1.skip(400).limit(1)[0]
        pivot2 = oplog2.skip(400).limit(1)[0]

        cursor1 = self.opman1.get_oplog_cursor(pivot1["ts"])
        cursor2 = self.opman2.get_oplog_cursor(pivot2["ts"])
        self.assertEqual(cursor1.count(), 1 + 1 + 998 - 400)
        self.assertEqual(cursor2.count(), 1 + 1002 - 400)

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
        self.opman2.oplog = self.shard1_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i + 500
            })
        last_ts1 = self.opman1.get_last_oplog_timestamp()
        last_ts2 = self.opman2.get_last_oplog_timestamp()
        self.assertEqual(last_ts1, self.opman1.dump_collection())
        self.assertEqual(last_ts2, self.opman2.dump_collection())
        self.assertEqual(len(self.opman1.doc_manager._search()),
                         len(self.opman2.doc_manager._search()))
        self.assertEqual(len(self.opman1.doc_manager._search()), 1000)

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

        # Some cleanup
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        self.opman1.checkpoint = self.opman2.checkpoint = None

    def test_rollback(self):
        """Test rollback in oplog_manager. Assertion failure if it doesn't pass
            We force a rollback by inserting a doc, killing primary, inserting
            another doc, killing the new primary, and then restarting both
            servers.
        """

        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        if not start_cluster(sharded=True):
            self.fail("Shards cannot be added to mongos")

        test_oplog, primary_conn, solr, mongos = self.get_new_oplog()

        solr = DocManager()
        test_oplog.doc_manager = solr
        solr._delete()          # equivalent to solr.delete(q='*:*')

        safe_mongo_op(mongos['alpha']['foo'].remove, {})
        safe_mongo_op(mongos['alpha']['foo'].insert,
                      {'_id': ObjectId('4ff74db3f646462b38000001'),
                      'name': 'paulie'})
        cutoff_ts = test_oplog.get_last_oplog_timestamp()

        obj2 = ObjectId('4ff74db3f646462b38000002')
        first_doc = {'name': 'paulie', '_ts': bson_ts_to_long(cutoff_ts),
                     'ns': 'alpha.foo', 
                     '_id': ObjectId('4ff74db3f646462b38000001')}

        # try kill one, try restarting
        kill_mongo_proc(primary_conn.host, PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                mongos['alpha']['foo'].insert({'_id': obj2, 'name': 'paul'})
                break
            except OperationFailure:
                time.sleep(1)
                count += 1
                if count > 60:
                    self.fail('Insert failed too many times in rollback')
                continue

        kill_mongo_proc(primary_conn.host, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)

        # wait for master to be established
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        # wait for secondary to be established
        admin_db = new_primary_conn['admin']
        while admin_db.command("replSetGetStatus")['myState'] != 2:
            time.sleep(1)

        while retry_until_ok(mongos['alpha']['foo'].find().count) != 1:
            time.sleep(1)

        self.assertEqual(str(new_primary_conn.port), PORTS_ONE['SECONDARY'])
        self.assertEqual(str(primary_conn.port), PORTS_ONE['PRIMARY'])

        last_ts = test_oplog.get_last_oplog_timestamp()
        second_doc = {'name': 'paul', '_ts': bson_ts_to_long(last_ts),
                      'ns': 'alpha.foo', '_id': obj2}

        test_oplog.doc_manager.upsert(first_doc)
        test_oplog.doc_manager.upsert(second_doc)
        test_oplog.rollback()
        test_oplog.doc_manager.commit()
        results = solr._search()

        self.assertEqual(len(results), 1)

        results_doc = results[0]
        self.assertEqual(results_doc['name'], 'paulie')
        self.assertTrue(results_doc['_ts'] <= bson_ts_to_long(cutoff_ts))


if __name__ == '__main__':
    unittest.main()
