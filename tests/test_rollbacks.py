"""Test Mongo Connector's behavior when its source MongoDB system is
experiencing a rollback.

"""

import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
import time

from pymongo.read_preferences import ReadPreference
from pymongo import MongoClient

from mongo_connector.util import retry_until_ok
from mongo_connector.locking_dict import LockingDict
from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.oplog_manager import OplogThread

from tests import mongo_host
from tests.util import assert_soon
from tests.setup_cluster import (
    start_replica_set,
    kill_all,
    kill_mongo_proc,
    restart_mongo_proc,
)


class TestRollbacks(unittest.TestCase):

    def tearDown(self):
        kill_all()

    def setUp(self):
        # Create a new oplog progress file
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()

        # Start a replica set
        _, self.secondary_p, self.primary_p = start_replica_set('rollbacks')
        # Connection to the replica set as a whole
        self.main_conn = MongoClient('%s:%d' % (mongo_host, self.primary_p),
                                     replicaSet='rollbacks')
        # Connection to the primary specifically
        self.primary_conn = MongoClient('%s:%d' % (mongo_host, self.primary_p))
        # Connection to the secondary specifically
        self.secondary_conn = MongoClient(
            '%s:%d' % (mongo_host, self.secondary_p),
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

        # Wipe any test data
        self.main_conn["test"]["mc"].drop()

        # Oplog thread
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        self.opman = OplogThread(
            primary_conn=self.main_conn,
            main_address='%s:%d' % (mongo_host, self.primary_p),
            oplog_coll=self.main_conn["local"]["oplog.rs"],
            is_sharded=False,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mc"],
            auth_key=None,
            auth_username=None,
            repl_set="rollbacks"
        )

    def test_single_target(self):
        """Test with a single replication target"""

        self.opman.start()

        # Insert first document with primary up
        self.main_conn["test"]["mc"].insert({"i": 0})
        self.assertEqual(self.primary_conn["test"]["mc"].find().count(), 1)

        # Make sure the insert is replicated
        secondary = self.secondary_conn
        assert_soon(lambda: secondary["test"]["mc"].count() == 1,
                    "first write didn't replicate to secondary")

        # Kill the primary
        kill_mongo_proc(self.primary_p, destroy=False)

        # Wait for the secondary to be promoted
        assert_soon(lambda: secondary["admin"].command("isMaster")["ismaster"])

        # Insert another document. This will be rolled back later
        retry_until_ok(self.main_conn["test"]["mc"].insert, {"i": 1})
        self.assertEqual(secondary["test"]["mc"].count(), 2)

        # Wait for replication to doc manager
        assert_soon(lambda: len(self.opman.doc_managers[0]._search()) == 2,
                    "not all writes were replicated to doc manager")

        # Kill the new primary
        kill_mongo_proc(self.secondary_p, destroy=False)

        # Start both servers back up
        restart_mongo_proc(self.primary_p)
        primary_admin = self.primary_conn["admin"]
        assert_soon(lambda: primary_admin.command("isMaster")["ismaster"],
                    "restarted primary never resumed primary status")
        restart_mongo_proc(self.secondary_p)
        assert_soon(lambda: retry_until_ok(secondary.admin.command,
                                           'replSetGetStatus')['myState'] == 2,
                    "restarted secondary never resumed secondary status")
        assert_soon(lambda:
                    retry_until_ok(self.main_conn.test.mc.find().count) > 0,
                    "documents not found after primary/secondary restarted")

        # Only first document should exist in MongoDB
        self.assertEqual(self.main_conn["test"]["mc"].count(), 1)
        self.assertEqual(self.main_conn["test"]["mc"].find_one()["i"], 0)

        # Same case should hold for the doc manager
        doc_manager = self.opman.doc_managers[0]
        self.assertEqual(len(doc_manager._search()), 1)
        self.assertEqual(doc_manager._search()[0]["i"], 0)

        # cleanup
        self.opman.join()

    def test_many_targets(self):
        """Test with several replication targets"""

        # OplogThread has multiple doc managers
        doc_managers = [DocManager(), DocManager(), DocManager()]
        self.opman.doc_managers = doc_managers

        self.opman.start()

        # Insert a document into each namespace
        self.main_conn["test"]["mc"].insert({"i": 0})
        self.assertEqual(self.primary_conn["test"]["mc"].count(), 1)

        # Make sure the insert is replicated
        secondary = self.secondary_conn
        assert_soon(lambda: secondary["test"]["mc"].count() == 1,
                    "first write didn't replicate to secondary")

        # Kill the primary
        kill_mongo_proc(self.primary_p, destroy=False)

        # Wait for the secondary to be promoted
        assert_soon(lambda: secondary.admin.command("isMaster")['ismaster'],
                    'secondary was never promoted')

        # Insert more documents. This will be rolled back later
        # Some of these documents will be manually removed from
        # certain doc managers, to emulate the effect of certain
        # target systems being ahead/behind others
        secondary_ids = []
        for i in range(1, 10):
            secondary_ids.append(
                retry_until_ok(self.main_conn["test"]["mc"].insert,
                               {"i": i}))
        self.assertEqual(self.secondary_conn["test"]["mc"].count(), 10)

        # Wait for replication to the doc managers
        def docmans_done():
            for dm in self.opman.doc_managers:
                if len(dm._search()) != 10:
                    return False
            return True
        assert_soon(docmans_done,
                    "not all writes were replicated to doc managers")

        # Remove some documents from the doc managers to simulate
        # uneven replication
        ts = self.opman.doc_managers[0].get_last_doc()['_ts']
        for id in secondary_ids[8:]:
            self.opman.doc_managers[1].remove({
                "_id": id,
                "ns": "test.mc",
                "_ts": ts
            })
        for id in secondary_ids[2:]:
            self.opman.doc_managers[2].remove({
                "_id": id,
                "ns": "test.mc",
                "_ts": ts
            })

        # Kill the new primary
        kill_mongo_proc(self.secondary_p, destroy=False)

        # Start both servers back up
        restart_mongo_proc(self.primary_p)
        primary_admin = self.primary_conn["admin"]
        assert_soon(lambda: primary_admin.command("isMaster")['ismaster'],
                    'restarted primary never resumed primary status')
        restart_mongo_proc(self.secondary_p)
        assert_soon(lambda: retry_until_ok(secondary.admin.command,
                                           'replSetGetStatus')['myState'] == 2,
                    "restarted secondary never resumed secondary status")
        assert_soon(lambda:
                    retry_until_ok(self.primary_conn.test.mc.find().count) > 0,
                    "documents not found after primary/secondary restarted")

        # Only first document should exist in MongoDB
        self.assertEqual(self.primary_conn["test"]["mc"].count(), 1)
        self.assertEqual(self.primary_conn["test"]["mc"].find_one()["i"], 0)

        # Give OplogThread some time to catch up
        time.sleep(10)

        # Same case should hold for the doc managers
        for dm in self.opman.doc_managers:
            self.assertEqual(len(dm._search()), 1)
            self.assertEqual(dm._search()[0]["i"], 0)

        self.opman.join()

    def test_deletions(self):
        """Test rolling back 'd' operations"""

        self.opman.start()

        # Insert a document, wait till it replicates to secondary
        self.main_conn["test"]["mc"].insert({"i": 0})
        self.main_conn["test"]["mc"].insert({"i": 1})
        self.assertEqual(self.primary_conn["test"]["mc"].find().count(), 2)
        assert_soon(lambda: self.secondary_conn["test"]["mc"].count() == 2,
                    "first write didn't replicate to secondary")

        # Kill the primary, wait for secondary to be promoted
        kill_mongo_proc(self.primary_p, destroy=False)
        assert_soon(lambda: self.secondary_conn["admin"]
                    .command("isMaster")["ismaster"])

        # Delete first document
        retry_until_ok(self.main_conn["test"]["mc"].remove, {"i": 0})
        self.assertEqual(self.secondary_conn["test"]["mc"].count(), 1)

        # Wait for replication to doc manager
        assert_soon(lambda: len(self.opman.doc_managers[0]._search()) == 1,
                    "delete was not replicated to doc manager")

        # Kill the new primary
        kill_mongo_proc(self.secondary_p, destroy=False)

        # Start both servers back up
        restart_mongo_proc(self.primary_p)
        primary_admin = self.primary_conn["admin"]
        assert_soon(lambda: primary_admin.command("isMaster")["ismaster"],
                    "restarted primary never resumed primary status")
        restart_mongo_proc(self.secondary_p)
        assert_soon(lambda: retry_until_ok(self.secondary_conn.admin.command,
                                           'replSetGetStatus')['myState'] == 2,
                    "restarted secondary never resumed secondary status")

        # Both documents should exist in mongo
        assert_soon(lambda: retry_until_ok(
            self.main_conn["test"]["mc"].count) == 2)

        # Both document should exist in doc manager
        doc_manager = self.opman.doc_managers[0]
        self.assertEqual(len(doc_manager._search()), 2)

        self.opman.join()
