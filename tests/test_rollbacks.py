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
from tests.util import wait_for
from tests.setup_cluster import (
    start_cluster,
    kill_all,
    kill_mongo_proc,
    start_mongo_proc,
    PORTS_ONE
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
        start_cluster(sharded=False, use_mongos=False)
        # Connection to the replica set as a whole
        self.main_conn = MongoClient("localhost:%s" % PORTS_ONE["PRIMARY"],
                                     replicaSet="demo-repl")
        # Connection to the primary specifically
        self.primary_conn = MongoClient("localhost:%s" % PORTS_ONE["PRIMARY"])
        # Connection to the secondary specifically
        self.secondary_conn = MongoClient(
            "localhost:%s" % PORTS_ONE["SECONDARY"],
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

        # Wipe any test data
        self.main_conn["test"]["mc"].drop()

        # Oplog thread
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        self.opman = OplogThread(
            primary_conn=self.main_conn,
            main_address="localhost:%s" % PORTS_ONE["PRIMARY"],
            oplog_coll=self.main_conn["local"]["oplog.rs"],
            is_sharded=False,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mc"],
            auth_key=None,
            auth_username=None,
            repl_set="demo-repl"
        )

    def test_single_target(self):
        """Test with a single replication target"""

        self.opman.start()

        # Insert first document with primary up
        self.main_conn["test"]["mc"].insert({"i": 0})
        self.assertEqual(self.primary_conn["test"]["mc"].find().count(), 1)

        # Make sure the insert is replicated
        secondary = self.secondary_conn
        self.assertTrue(wait_for(lambda: secondary["test"]["mc"].count() == 1),
                        "first write didn't replicate to secondary")

        # Kill the primary
        kill_mongo_proc("localhost", PORTS_ONE["PRIMARY"])

        # Wait for the secondary to be promoted
        while not secondary["admin"].command("isMaster")["ismaster"]:
            time.sleep(1)

        # Insert another document. This will be rolled back later
        retry_until_ok(self.main_conn["test"]["mc"].insert, {"i": 1})
        self.assertEqual(secondary["test"]["mc"].count(), 2)

        # Wait for replication to doc manager
        c = lambda: len(self.opman.doc_managers[0]._search()) == 2
        self.assertTrue(wait_for(c),
                        "not all writes were replicated to doc manager")

        # Kill the new primary
        kill_mongo_proc("localhost", PORTS_ONE["SECONDARY"])

        # Start both servers back up
        start_mongo_proc(
            port=PORTS_ONE['PRIMARY'],
            repl_set_name="demo-repl",
            data="replset1a",
            log="replset1a.log"
        )
        primary_admin = self.primary_conn["admin"]
        while not primary_admin.command("isMaster")["ismaster"]:
            time.sleep(1)
        start_mongo_proc(
            port=PORTS_ONE['SECONDARY'],
            repl_set_name="demo-repl",
            data="replset1b",
            log="replset1b.log"
        )
        while secondary["admin"].command("replSetGetStatus")["myState"] != 2:
            time.sleep(1)
        while retry_until_ok(self.main_conn["test"]["mc"].find().count) == 0:
            time.sleep(1)

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
        self.assertTrue(wait_for(lambda: secondary["test"]["mc"].count() == 1),
                        "first write didn't replicate to secondary")

        # Kill the primary
        kill_mongo_proc("localhost", PORTS_ONE["PRIMARY"])

        # Wait for the secondary to be promoted
        while not secondary["admin"].command("isMaster")["ismaster"]:
            time.sleep(1)

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
        self.assertTrue(wait_for(docmans_done),
                        "not all writes were replicated to doc managers")

        # Remove some documents from the doc managers to simulate
        # uneven replication
        for id in secondary_ids[8:]:
            self.opman.doc_managers[1].remove({"_id": id})
        for id in secondary_ids[2:]:
            self.opman.doc_managers[2].remove({"_id": id})

        # Kill the new primary
        kill_mongo_proc("localhost", PORTS_ONE["SECONDARY"])

        # Start both servers back up
        start_mongo_proc(
            port=PORTS_ONE['PRIMARY'],
            repl_set_name="demo-repl",
            data="replset1a",
            log="replset1a.log"
        )
        primary_admin = self.primary_conn["admin"]
        while not primary_admin.command("isMaster")["ismaster"]:
            time.sleep(1)
        start_mongo_proc(
            port=PORTS_ONE['SECONDARY'],
            repl_set_name="demo-repl",
            data="replset1b",
            log="replset1b.log"
        )
        while secondary["admin"].command("replSetGetStatus")["myState"] != 2:
            time.sleep(1)
        while retry_until_ok(self.primary_conn["test"]["mc"].find().count) == 0:
            time.sleep(1)

        # Only first document should exist in MongoDB
        self.assertEqual(self.primary_conn["test"]["mc"].count(), 1)
        self.assertEqual(self.primary_conn["test"]["mc"].find_one()["i"], 0)

        # Same case should hold for the doc managers
        for dm in self.opman.doc_managers:
            self.assertEqual(len(dm._search()), 1)
            self.assertEqual(dm._search()[0]["i"], 0)

        self.opman.join()
