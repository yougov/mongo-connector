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

    @classmethod
    def get_oplog_thread(cls):
        """ Set up connection with mongo.

        Returns oplog, the connection and oplog collection.
        This function clears the oplog.
        """
        primary_conn = Connection(HOSTNAME,int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE["MONGOS"])
        mongos = Connection(mongos_addr)
        mongos['alpha']['foo'].drop()

        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog

        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=1000000)
        namespace_set = ['test.test', 'alpha.foo']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, True,
                            doc_manager, LockingDict(), namespace_set,
                            cls.AUTH_KEY, AUTH_USERNAME)

        return (oplog, primary_conn, oplog_coll, mongos)

    @classmethod
    def get_new_oplog(cls):
        """ Set up connection with mongo.

        Returns oplog, the connection and oplog collection
        This function does not clear the oplog
        """
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        mongos = "%s:%s" % (HOSTNAME, PORTS_ONE["MONGOS"])
        oplog_coll = primary_conn['local']['oplog.rs']

        namespace_set = ['test.test', 'alpha.foo']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos, oplog_coll, True,
                            doc_manager, LockingDict(), namespace_set,
                            cls.AUTH_KEY, AUTH_USERNAME)

        return (oplog, primary_conn, oplog_coll, oplog.main_connection)

    def test_retrieve_doc(self):
        """Test retrieve_doc in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, oplog_cursor, oplog_coll, mongos = self.get_oplog_thread()
        # testing for entry as none type
        entry = None
        assert (test_oplog.retrieve_doc(entry) is None)

        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)

        assert (oplog_cursor.count() == 0)

        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        target_entry = mongos['alpha']['foo'].find_one()

        # testing for search after inserting a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) == target_entry)

        safe_mongo_op(mongos['alpha']['foo'].update, {'name': 'paulie'},
                      {"$set": {'name': 'paul'}})
        last_oplog_entry = next(oplog_cursor)
        target_entry = mongos['alpha']['foo'].find_one()

        # testing for search after updating a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) == target_entry)

        safe_mongo_op(mongos['alpha']['foo'].remove, {'name': 'paul'})
        last_oplog_entry = next(oplog_cursor)

        # testing for search after deleting a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) is None)

        last_oplog_entry['o']['_id'] = 'badID'

        # testing for bad doc id as input
        assert (test_oplog.retrieve_doc(last_oplog_entry) is None)

        # test_oplog.stop()

    def test_get_oplog_cursor(self):
        """Test get_oplog_cursor in oplog_manager.

        Assertion failure if it doesn't pass
        """
        test_oplog, timestamp, cursor, mongos = self.get_oplog_thread()
        # test None cursor
        assert (test_oplog.get_oplog_cursor(None) is None)

        # test with one document
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        timestamp = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.get_oplog_cursor(timestamp)
        assert (cursor.count() == 1)

        # test with two documents, one after the ts
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paul'})
        cursor = test_oplog.get_oplog_cursor(timestamp)
        assert (cursor.count() == 2)


    def test_get_last_oplog_timestamp(self):
        """Test get_last_oplog_timestamp in oplog_manager.

        Assertion failure if it doesn't pass
        """

        # test empty oplog
        test_oplog, oplog_cursor, oplog_coll, mongos = self.get_oplog_thread()

        assert (test_oplog.get_last_oplog_timestamp() is None)

        # test non-empty oplog
        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        last_ts = last_oplog_entry['ts']
        assert (test_oplog.get_last_oplog_timestamp() == last_ts)

        # test_oplog.stop()


    def test_dump_collection(self):
        """Test dump_collection in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, search_ts, solr, mongos = self.get_oplog_thread()
        solr = DocManager()
        test_oplog.doc_manager = solr

        # with documents
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        test_oplog.dump_collection()

        test_oplog.doc_manager.commit()
        solr_results = solr._search()
        assert (len(solr_results) == 1)
        solr_doc = solr_results[0]
        assert (long_to_bson_ts(solr_doc['_ts']) == search_ts)
        assert (solr_doc['name'] == 'paulie')
        assert (solr_doc['ns'] == 'alpha.foo')


    def test_init_cursor(self):
        """Test init_cursor in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, search_ts, cursor, mongos = self.get_oplog_thread()
        test_oplog.checkpoint = None           # needed for these tests

        # initial tests with no config file and empty oplog
        assert (test_oplog.init_cursor() is None)

        # no config, single oplog entry
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.init_cursor()

        assert (cursor.count() == 1)
        assert (test_oplog.checkpoint == search_ts)

        # with config file, assert that size != 0
        os.system('touch %s' % (TEMP_CONFIG))

        cursor = test_oplog.init_cursor()
        oplog_dict = test_oplog.oplog_progress.get_dict()

        assert(cursor.count() == 1)
        self.assertTrue(str(test_oplog.oplog) in oplog_dict)
        commit_ts = test_oplog.checkpoint
        self.assertTrue(oplog_dict[str(test_oplog.oplog)] == commit_ts)

        os.system('rm %s' % (TEMP_CONFIG))

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
