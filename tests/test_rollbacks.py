# Copyright 2013-2016 MongoDB, Inc.
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

"""Test Mongo Connector's behavior when its source MongoDB system is
experiencing a rollback.

"""

import os
import sys
import time

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
    ReplicaSet,
    STRESS_COUNT,
)
from mongo_connector.util import retry_until_ok
from tests import unittest


class TestRollbacks(unittest.TestCase):
    def tearDown(self):
        close_client(self.primary_conn)
        close_client(self.secondary_conn)
        try:
            self.opman.join()
        except RuntimeError:
            # OplogThread may not have been started
            pass
        self.repl_set.stop()

    def setUp(self):
        # Create a new oplog progress file
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        open("oplog.timestamp", "w").close()

        # Start a replica set
        self.repl_set = ReplicaSet().start()
        # Connection to the replica set as a whole
        self.main_conn = self.repl_set.client()
        # Connection to the primary specifically
        self.primary_conn = self.repl_set.primary.client()
        # Connection to the secondary specifically
        self.secondary_conn = self.repl_set.secondary.client(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

        # Wipe any test data
        self.main_conn.drop_database("test")

        # Oplog thread
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        self.opman = OplogThread(
            primary_client=self.main_conn,
            doc_managers=(doc_manager,),
            oplog_progress_dict=oplog_progress,
            namespace_config=NamespaceConfig(namespace_set=["test.mc"]),
        )

    def test_single_target(self):
        """Test with a single replication target"""

        self.opman.start()

        # Insert first document with primary up
        self.main_conn["test"]["mc"].insert_one({"i": 0})
        self.assertEqual(self.primary_conn["test"]["mc"].find().count(), 1)

        # Make sure the insert is replicated
        secondary = self.secondary_conn
        assert_soon(
            lambda: secondary["test"]["mc"].count() == 1,
            "first write didn't replicate to secondary",
        )

        # Kill the primary
        self.repl_set.primary.stop(destroy=False)

        # Wait for the secondary to be promoted
        assert_soon(lambda: secondary["admin"].command("isMaster")["ismaster"])

        # Insert another document. This will be rolled back later
        retry_until_ok(self.main_conn["test"]["mc"].insert_one, {"i": 1})
        self.assertEqual(secondary["test"]["mc"].count(), 2)

        # Wait for replication to doc manager
        assert_soon(
            lambda: len(self.opman.doc_managers[0]._search()) == 2,
            "not all writes were replicated to doc manager",
        )

        # Kill the new primary
        self.repl_set.secondary.stop(destroy=False)

        # Start both servers back up
        self.repl_set.primary.start()
        primary_admin = self.primary_conn["admin"]
        assert_soon(
            lambda: primary_admin.command("isMaster")["ismaster"],
            "restarted primary never resumed primary status",
        )
        self.repl_set.secondary.start()
        assert_soon(
            lambda: retry_until_ok(secondary.admin.command, "replSetGetStatus")[
                "myState"
            ]
            == 2,
            "restarted secondary never resumed secondary status",
        )
        assert_soon(
            lambda: retry_until_ok(self.main_conn.test.mc.find().count) > 0,
            "documents not found after primary/secondary restarted",
        )

        # Only first document should exist in MongoDB
        self.assertEqual(self.main_conn["test"]["mc"].count(), 1)
        self.assertEqual(self.main_conn["test"]["mc"].find_one()["i"], 0)

        # Same case should hold for the doc manager
        doc_manager = self.opman.doc_managers[0]
        assert_soon(
            lambda: len(doc_manager._search()) == 1,
            "documents never rolled back in doc manager.",
        )
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
        self.main_conn["test"]["mc"].insert_one({"i": 0})
        self.assertEqual(self.primary_conn["test"]["mc"].count(), 1)

        # Make sure the insert is replicated
        secondary = self.secondary_conn
        assert_soon(
            lambda: secondary["test"]["mc"].count() == 1,
            "first write didn't replicate to secondary",
        )

        # Kill the primary
        self.repl_set.primary.stop(destroy=False)

        # Wait for the secondary to be promoted
        assert_soon(
            lambda: secondary.admin.command("isMaster")["ismaster"],
            "secondary was never promoted",
        )

        # Insert more documents. This will be rolled back later
        # Some of these documents will be manually removed from
        # certain doc managers, to emulate the effect of certain
        # target systems being ahead/behind others
        secondary_ids = []
        for i in range(1, 10):
            secondary_ids.append(
                retry_until_ok(
                    self.main_conn["test"]["mc"].insert_one, {"i": i}
                ).inserted_id
            )
        self.assertEqual(self.secondary_conn["test"]["mc"].count(), 10)

        # Wait for replication to the doc managers
        def docmans_done():
            for dm in self.opman.doc_managers:
                if len(dm._search()) != 10:
                    return False
            return True

        assert_soon(docmans_done, "not all writes were replicated to doc managers")

        # Remove some documents from the doc managers to simulate
        # uneven replication
        ts = self.opman.doc_managers[0].get_last_doc()["_ts"]
        for id in secondary_ids[8:]:
            self.opman.doc_managers[1].remove(id, "test.mc", ts)
        for id in secondary_ids[2:]:
            self.opman.doc_managers[2].remove(id, "test.mc", ts)

        # Kill the new primary
        self.repl_set.secondary.stop(destroy=False)

        # Start both servers back up
        self.repl_set.primary.start()
        primary_admin = self.primary_conn["admin"]
        assert_soon(
            lambda: primary_admin.command("isMaster")["ismaster"],
            "restarted primary never resumed primary status",
        )
        self.repl_set.secondary.start()
        assert_soon(
            lambda: retry_until_ok(secondary.admin.command, "replSetGetStatus")[
                "myState"
            ]
            == 2,
            "restarted secondary never resumed secondary status",
        )
        assert_soon(
            lambda: retry_until_ok(self.primary_conn.test.mc.find().count) > 0,
            "documents not found after primary/secondary restarted",
        )

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
        self.main_conn["test"]["mc"].insert_one({"i": 0})
        self.main_conn["test"]["mc"].insert_one({"i": 1})
        self.assertEqual(self.primary_conn["test"]["mc"].find().count(), 2)
        assert_soon(
            lambda: self.secondary_conn["test"]["mc"].count() == 2,
            "first write didn't replicate to secondary",
        )

        # Kill the primary, wait for secondary to be promoted
        self.repl_set.primary.stop(destroy=False)
        assert_soon(
            lambda: self.secondary_conn["admin"].command("isMaster")["ismaster"]
        )

        # Delete first document
        retry_until_ok(self.main_conn["test"]["mc"].delete_one, {"i": 0})
        self.assertEqual(self.secondary_conn["test"]["mc"].count(), 1)

        # Wait for replication to doc manager
        assert_soon(
            lambda: len(self.opman.doc_managers[0]._search()) == 1,
            "delete was not replicated to doc manager",
        )

        # Kill the new primary
        self.repl_set.secondary.stop(destroy=False)

        # Start both servers back up
        self.repl_set.primary.start()
        primary_admin = self.primary_conn["admin"]
        assert_soon(
            lambda: primary_admin.command("isMaster")["ismaster"],
            "restarted primary never resumed primary status",
        )
        self.repl_set.secondary.start()
        assert_soon(
            lambda: retry_until_ok(
                self.secondary_conn.admin.command, "replSetGetStatus"
            )["myState"]
            == 2,
            "restarted secondary never resumed secondary status",
        )

        # Both documents should exist in mongo
        assert_soon(lambda: retry_until_ok(self.main_conn["test"]["mc"].count) == 2)

        # Both document should exist in doc manager
        doc_manager = self.opman.doc_managers[0]
        assert_soon(
            lambda: len(list(doc_manager._search())) == 2,
            ("Expected two documents, but got: %r" % list(doc_manager._search())),
        )

        self.opman.join()

    def test_stressed_rollback(self):
        """Stress test for a rollback with many documents."""
        self.opman.start()

        c = self.main_conn.test.mc
        docman = self.opman.doc_managers[0]
        c2 = c.with_options(write_concern=WriteConcern(w=2))
        c2.insert_many([{"i": i} for i in range(STRESS_COUNT)])
        assert_soon(lambda: c2.count() == STRESS_COUNT)

        def condition():
            return len(docman._search()) == STRESS_COUNT

        assert_soon(
            condition,
            (
                "Was expecting %d documents in DocManager, "
                "but %d found instead." % (STRESS_COUNT, len(docman._search()))
            ),
        )

        primary_conn = self.repl_set.primary.client()
        self.repl_set.primary.stop(destroy=False)
        new_primary_conn = self.repl_set.secondary.client()

        admin = new_primary_conn.admin
        assert_soon(lambda: retry_until_ok(admin.command, "isMaster")["ismaster"])

        retry_until_ok(
            c.insert_many, [{"i": str(STRESS_COUNT + i)} for i in range(STRESS_COUNT)]
        )

        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()
        admin = primary_conn.admin
        assert_soon(lambda: retry_until_ok(admin.command, "isMaster")["ismaster"])
        self.repl_set.secondary.start()

        assert_soon(lambda: retry_until_ok(c.count) == STRESS_COUNT)
        assert_soon(
            condition,
            (
                "Was expecting %d documents in DocManager, "
                "but %d found instead." % (STRESS_COUNT, len(docman._search()))
            ),
        )

        self.opman.join()


if __name__ == "__main__":
    unittest.main()
