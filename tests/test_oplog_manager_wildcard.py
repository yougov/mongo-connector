# Copyright 2016 MongoDB, Inc.
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

"""Test oplog manager methods with wildcard namespaces
"""

import itertools
import re
import sys
import time

import bson
import pymongo

sys.path[0:0] = [""]  # noqa

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.test_utils import assert_soon, close_client, ReplicaSetSingle
from tests import unittest


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
    cluster
    """

    def setUp(self):
        self.repl_set = ReplicaSetSingle().start()
        self.primary_conn = self.repl_set.client()
        self.oplog_coll = self.primary_conn.local["oplog.rs"]

    def reset_opman(self, include_ns=None, exclude_ns=None, dest_mapping=None):
        self.namespace_config = NamespaceConfig(
            namespace_set=include_ns,
            ex_namespace_set=exclude_ns,
            namespace_options=dest_mapping,
        )
        self.opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            namespace_config=self.namespace_config,
        )

    def init_dbs(self):
        # includedb1.* & includedb2.includecol1 are interested collections
        self.primary_conn["includedb1"]["includecol1"].insert_many(
            [{"idb1col1": i} for i in range(1, 3)]
        )

        self.primary_conn["includedb1"]["includecol2"].insert_many(
            [{"idb1col2": i} for i in range(1, 3)]
        )

        self.primary_conn["includedb2"]["includecol1"].insert_many(
            [{"idb2col1": i} for i in range(1, 3)]
        )

        # the others are not interested collections
        self.primary_conn["includedb2"]["excludecol2"].insert_many(
            [{"idb2col2": i} for i in range(1, 3)]
        )

        self.primary_conn["excludedb3"]["excludecol1"].insert_many(
            [{"idb3col1": i} for i in range(1, 3)]
        )

    def tearDown(self):
        try:
            self.opman.join()
        except RuntimeError:
            pass  # OplogThread may not have been started

        for db in self.primary_conn.database_names():
            if db != "local":
                self.primary_conn.drop_database(db)
        close_client(self.primary_conn)
        self.repl_set.stop()

    def test_get_oplog_cursor(self):
        """Test the get_oplog_cursor method"""

        # Put something in the dbs
        self.init_dbs()

        # timestamp is None - all oplog entries excluding no-ops are returned.
        # wildcard include case no impact the result
        self.reset_opman(["includedb1.*", "includedb2.includecol1"], [], {})

        got_cursor = self.opman.get_oplog_cursor(None)
        oplog_cursor = self.oplog_coll.find({"op": {"$ne": "n"}})
        self.assertNotEqual(got_cursor, None)
        self.assertEqual(got_cursor.count(), oplog_cursor.count())

        # wildcard exclude case no impact the result
        self.reset_opman([], ["includedb2.excludecol2", "excludedb3.*"], {})

        got_cursor = self.opman.get_oplog_cursor(None)
        oplog_cursor = self.oplog_coll.find({"op": {"$ne": "n"}})
        self.assertNotEqual(got_cursor, None)
        self.assertEqual(got_cursor.count(), oplog_cursor.count())

        # earliest entry is the only one at/after timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "idb1col1": 1}
        self.primary_conn["includedb1"]["includecol1"].insert_one(doc)
        latest_timestamp = self.opman.get_last_oplog_timestamp()
        cursor = self.opman.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count(), 1)
        next_entry_id = next(cursor)["o"]["_id"]
        retrieved = self.primary_conn.includedb1.includecol1.find_one(next_entry_id)
        self.assertEqual(retrieved, doc)

        # many entries before and after timestamp
        self.primary_conn["includedb1"]["includecol1"].insert_many(
            [{"idb1col1": i} for i in range(2, 1002)]
        )
        oplog_cursor = self.oplog_coll.find(
            {"op": {"$ne": "n"}, "ns": {"$not": re.compile(r"\.(system|\$cmd)")}},
            sort=[("ts", pymongo.ASCENDING)],
        )

        # initial insert + 1000 more inserts
        self.assertEqual(oplog_cursor.count(), 11 + 1000)
        pivot = oplog_cursor.skip(400).limit(-1)[0]

        goc_cursor = self.opman.get_oplog_cursor(pivot["ts"])
        self.assertEqual(goc_cursor.count(), 11 + 1000 - 400)

    def test_get_last_oplog_timestamp(self):
        """Test the get_last_oplog_timestamp method"""

        # empty oplog case has been tested in test_oplog_manager.py,
        # skip that here.

        # Put something in the dbs
        self.init_dbs()

        # Test non-empty oplog
        self.reset_opman(["includedb1.*", "includedb2.includecol1"], [], {})
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.primary_conn["includedb1"]["includecol1"].insert_one(
                {"idb1col1": i + 500}
            )
        oplog = self.primary_conn["local"]["oplog.rs"]
        oplog = (
            oplog.find({"op": {"$ne": "n"}})
            .sort("$natural", pymongo.DESCENDING)
            .limit(-1)[0]
        )
        self.assertEqual(self.opman.get_last_oplog_timestamp(), oplog["ts"])

    def test_dump_collection(self):
        """Test the dump_collection method

        Cases:

        1. no namespace set is set
        2. include namespace set is set
        3. exclude namespace set is set

        empty oplog case has been tested in test_oplog_manager.py,
        skip that here.
        """

        # Put something in the dbs
        self.init_dbs()

        # no namespace set is set
        self.reset_opman([], [], {})
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        self.assertEqual(len(self.opman.doc_managers[0]._search()), 10)

        # include namespace set is set
        self.reset_opman(["includedb1.*", "includedb2.includecol1"], [], {})
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        self.assertEqual(len(self.opman.doc_managers[0]._search()), 6)

        # exclude namespace set is set
        self.reset_opman([], ["includedb2.excludecol2", "excludedb3.*"], {})
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        self.assertEqual(len(self.opman.doc_managers[0]._search()), 6)

    def test_dump_collection_with_error(self):
        """Test the dump_collection method with invalid documents.

        Cases:

        1. non-empty oplog, continue_on_error=True, invalid documents
        """

        self.reset_opman(["includedb1.*", "includedb2.includecol1"], [], {})
        # non-empty oplog, continue_on_error=True, invalid documents
        self.opman.continue_on_error = True
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]

        docs = [{"a": i} for i in range(100)]
        for i in range(50, 60):
            docs[i]["_upsert_exception"] = True
        self.primary_conn["includedb1"]["includecol3"].insert_many(docs)

        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        docs = self.opman.doc_managers[0]._search()
        docs = list(filter(lambda doc: "a" in doc, docs))
        docs.sort(key=lambda doc: doc["a"])

        self.assertEqual(len(docs), 90)
        expected_a = itertools.chain(range(0, 50), range(60, 100))
        for doc, correct_a in zip(docs, expected_a):
            self.assertEqual(doc["a"], correct_a)

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

        self.reset_opman(["includedb1.*", "includedb2.includecol1"], [], {})

        # No last checkpoint, no collection dump, nothing in oplog
        # "change oplog collection" to put nothing in oplog
        self.opman.oplog = self.primary_conn["includedb1"]["emptycollection"]
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
        collection = self.primary_conn["includedb1"]["includecol1"]
        collection.insert_one({"idb1col1": 1})
        collection.delete_one({"idb1col1": 1})
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
        collection.insert_one({"idb1col1": 2})
        cursor, cursor_empty = self.opman.init_cursor()
        for doc in cursor:
            last_doc = doc
        self.assertEqual(last_doc["o"]["idb1col1"], 2)
        self.assertIsNone(self.opman.checkpoint)

        # Last checkpoint exists
        collection.insert_many([{"idb1col1": i + 500} for i in range(1000)])
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
        included_names = [
            "includedb1.includecol1",
            "includedb1.includecol2",
            "includedb2.includecol1",
        ]
        excluded_names = ["includedb2.excludecol2", "excludedb3.excludecol1"]
        dest_mapping = {
            "includedb1.*": "newdb1.*",
            "includedb2.includecol1": "newdb2.newcol1",
        }
        self.reset_opman(dest_mapping=dest_mapping)
        docman = self.opman.doc_managers[0]
        namespace_config = self.opman.namespace_config

        # start replicating
        self.opman.start()

        base_doc = {"_id": 1, "name": "superman"}

        for ns in included_names:
            db, coll = ns.split(".", 1)

            # test insert
            self.primary_conn[db][coll].insert_one(base_doc)

            assert_soon(lambda: len(docman._search()) == 1)
            self.assertEqual(
                docman._search()[0]["ns"], namespace_config.map_namespace(ns)
            )
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
            self.assertEqual(
                docman._search()[0]["ns"], namespace_config.map_namespace(ns)
            )
            bad = [d for d in docman._search() if d["ns"] == ns]
            self.assertEqual(len(bad), 0)

            # test delete
            self.primary_conn[db][coll].delete_one({"_id": 1})
            assert_soon(lambda: len(docman._search()) == 0)
            bad = [
                d
                for d in docman._search()
                if d["ns"] == namespace_config.map_namespace(ns)
            ]
            self.assertEqual(len(bad), 0)

            # cleanup
            self.primary_conn[db][coll].drop()
            self.opman.doc_managers[0]._delete()

        # doc not in namespace set
        for ns in excluded_names:
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

        self.reset_opman(["includedb1.*"], [], {})
        doc_managers = [DocManager(), DocManager(), DocManager()]
        self.opman.doc_managers = doc_managers

        # start replicating
        self.opman.start()
        self.primary_conn["includedb1"]["includecol1"].insert_one(
            {"name": "kermit", "color": "green"}
        )
        self.primary_conn["includedb1"]["includecol2"].insert_one(
            {"name": "elmo", "color": "firetruck red"}
        )
        self.primary_conn["excludedb2"]["excludecol1"].insert_one(
            {"name": "panda", "color": "white and black"}
        )

        assert_soon(
            lambda: sum(len(d._search()) for d in doc_managers) == 6,
            "OplogThread should be able to replicate to multiple targets",
        )

        self.primary_conn["includedb1"]["includecol2"].delete_one({"name": "elmo"})

        assert_soon(
            lambda: sum(len(d._search()) for d in doc_managers) == 3,
            "OplogThread should be able to replicate to multiple targets",
        )
        for d in doc_managers:
            self.assertEqual(d._search()[0]["name"], "kermit")


if __name__ == "__main__":
    unittest.main()
