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

import time
import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

import bson
import pymongo

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.utils import long_to_bson_ts
from tests.setup_cluster import (start_cluster,
                                 kill_all,
                                 PORTS_ONE)
from tests.util import assert_soon


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """

    def setUp(self):
        self.assertTrue(start_cluster(), "cluster could not start")
        self.primary_conn = pymongo.MongoClient(PORTS_ONE['PRIMARY'])
        self.oplog_coll = self.primary_conn.local['oplog.rs']
        self.opman = OplogThread(
            primary_conn=self.primary_conn,
            main_address=PORTS_ONE['PRIMARY'],
            oplog_coll=self.oplog_coll,
            is_sharded=False,
            doc_manager=DocManager(),
            oplog_progress_dict=LockingDict(),
            namespace_set=None,
            auth_key=None,      # TODO: support auth in this test
            auth_username=None
        )

    def tearDown(self):
        self.opman.join()
        self.primary_conn.close()
        kill_all()

    def test_retrieve_doc(self):
        """Test retrieve_doc in oplog_manager. Assertion failure if it doesn't
            pass
        """
        #testing for entry as none type
        entry = None
        self.assertTrue(self.opman.retrieve_doc(entry) is None)

        oplog_cursor = self.oplog_coll.find({}, tailable=True, await_data=True)

        self.primary_conn['test']['test'].insert({'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        target_entry = self.primary_conn['test']['test'].find_one()

        #testing for search after inserting a document
        self.assertEqual(self.opman.retrieve_doc(last_oplog_entry),
                         target_entry)

        self.primary_conn['test']['test'].update({'name': 'paulie'},
                                                 {"$set":  {'name': 'paul'}})

        last_oplog_entry = next(oplog_cursor)
        target_entry = self.primary_conn['test']['test'].find_one()

        #testing for search after updating a document
        self.assertEqual(self.opman.retrieve_doc(last_oplog_entry),
                         target_entry)

        self.primary_conn['test']['test'].remove({'name': 'paul'})
        last_oplog_entry = next(oplog_cursor)

        #testing for search after deleting a document
        self.assertEqual(self.opman.retrieve_doc(last_oplog_entry), None)

        last_oplog_entry['o']['_id'] = 'badID'

        #testing for bad doc id as input
        self.assertEqual(self.opman.retrieve_doc(last_oplog_entry), None)

    def test_get_oplog_cursor(self):
        '''Test the get_oplog_cursor method'''

        # Trivial case: timestamp is None
        self.assertEqual(self.opman.get_oplog_cursor(None), None)

        # earliest entry is after given timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.primary_conn["test"]["mcsharded"].insert(doc)
        self.assertEqual(self.opman.get_oplog_cursor(
            bson.Timestamp(1, 0)), None)

        # earliest entry is the only one at/after timestamp
        latest_timestamp = self.opman.get_last_oplog_timestamp()
        cursor = self.opman.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(self.opman.retrieve_doc(cursor[0]), doc)

        # many entries before and after timestamp
        self.primary_conn["test"]["mcsharded"].insert(
            {"i": i} for i in range(2, 2002))
        oplog_cursor = self.oplog_coll.find(
            sort=[("ts", pymongo.ASCENDING)]
        )

        # startup message + insert at beginning of tests + many inserts
        self.assertEqual(oplog_cursor.count(), 1 + 1 + 998)
        pivot = oplog_cursor.skip(400).limit(1)[0]

        goc_cursor = self.opman.get_oplog_cursor(pivot["ts"])
        self.assertEqual(goc_cursor.count(), 1 + 1 + 998 - 400)

        # get_oplog_cursor fast-forwards *one doc beyond* the given timestamp
        doc = self.primary_conn["test"]["mcsharded"].find_one(
            {"_id": next(goc_cursor)["o"]["_id"]})
        self.assertEqual(doc["i"], self.opman.retrieve_doc(pivot)["i"] + 1)

    def test_get_last_oplog_timestamp(self):
        """Test get_last_oplog_timestamp in oplog_manager. Assertion failure
            if it doesn't pass
        """

        #test empty oplog
        self.assertEqual(self.opman.get_last_oplog_timestamp(), None)

        #test non-empty oplog
        oplog_cursor = self.oplog_coll.find({}, tailable=True, await_data=True)
        self.primary_conn['test']['test'].insert({'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        self.assertEqual(self.opman.get_last_oplog_timestamp(),
                         last_oplog_entry['ts'])

    def test_dump_collection(self):
        """Test dump_collection in oplog_manager. Assertion failure if it
            doesn't pass
        """
        #with documents
        self.primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = self.opman.get_last_oplog_timestamp()
        self.opman.dump_collection()

        doc_manager = self.opman.doc_managers[0]
        doc_manager.commit()
        solr_results = doc_manager._search()
        self.assertEqual(len(solr_results), 1)
        solr_doc = solr_results[0]
        self.assertEqual(long_to_bson_ts(solr_doc['_ts']), search_ts)
        self.assertEqual(solr_doc['name'], 'paulie')
        self.assertEqual(solr_doc['ns'], 'test.test')

        # test multiple targets
        doc_managers = [DocManager(), DocManager(), DocManager()]
        self.opman.doc_managers = doc_managers
        self.primary_conn["test"]["test"].remove()
        for i in range(1000):
            self.primary_conn["test"]["test"].insert({"i": i})
        self.opman.dump_collection()
        for dm in doc_managers:
            self.assertEqual(len(dm._search()), 1000)

    def test_init_cursor(self):
        """Test init_cursor in oplog_manager. Assertion failure if it
            doesn't pass
        """
        self.opman.checkpoint = None  # needed for these tests

        # initial tests with no config file and empty oplog
        self.assertEqual(self.opman.init_cursor(), None)

        # no config, single oplog entry
        self.primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = self.opman.get_last_oplog_timestamp()
        cursor = self.opman.init_cursor()
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(self.opman.checkpoint, search_ts)

        # with config file, assert that size != 0
        os.system('touch temp_config.txt')

        cursor = self.opman.init_cursor()
        oplog_dict = self.opman.oplog_progress.get_dict()

        self.assertEqual(cursor.count(), 1)
        self.assertTrue(str(self.opman.oplog) in oplog_dict)
        self.assertTrue(oplog_dict[str(self.opman.oplog)] ==
                        self.opman.checkpoint)

        os.system('rm temp_config.txt')

        # test init_cursor when OplogThread created with/without no-dump option
        # insert some documents (will need to be dumped)
        self.primary_conn['test']['test'].remove()
        self.primary_conn['test']['test'].insert(
            {"_id": i} for i in range(100))

        # test no-dump option
        docman = self.opman.doc_managers[0]
        docman._delete()
        self.opman.collection_dump = False
        self.opman.oplog_progress = LockingDict()
        # init_cursor has the side-effect of causing a collection dump
        self.opman.init_cursor()
        self.assertEqual(len(docman._search()), 0)

        # test w/o no-dump option
        docman._delete()
        self.opman.collection_dump = True
        self.opman.oplog_progress = LockingDict()
        self.opman.init_cursor()
        self.assertEqual(len(docman._search()), 100)

    def test_filter_fields(self):
        docman = self.opman.doc_managers[0]
        conn = self.opman.main_connection

        include_fields = ["a", "b", "c"]
        exclude_fields = ["d", "e", "f"]

        # Set fields to care about
        self.opman.fields = include_fields
        # Documents have more than just these fields
        doc = {
            "a": 1, "b": 2, "c": 3,
            "d": 4, "e": 5, "f": 6,
            "_id": 1
        }
        db = conn['test']['test']
        db.insert(doc)
        assert_soon(lambda: db.count() == 1)
        self.opman.dump_collection()

        result = docman._search()[0]
        keys = result.keys()
        for inc, exc in zip(include_fields, exclude_fields):
            self.assertIn(inc, keys)
            self.assertNotIn(exc, keys)

    def test_namespace_mapping(self):
        """Test mapping of namespaces
        Cases:

        upsert/delete/update of documents:
        1. in namespace set, mapping provided
        2. outside of namespace set, mapping provided
        """

        source_ns = ["test.test1", "test.test2"]
        phony_ns = ["test.phony1", "test.phony2"]
        dest_mapping = {"test.test1": "test.test1_dest",
                        "test.test2": "test.test2_dest"}
        self.opman.dest_mapping = dest_mapping
        self.opman.namespace_set = source_ns
        # start replicating
        self.opman.start()

        base_doc = {"_id": 1, "name": "superman"}

        # doc in namespace set
        for ns in source_ns:
            db, coll = ns.split(".", 1)

            # test insert
            self.primary_conn[db][coll].insert(base_doc)

            assert_soon(lambda: len(self.docman._search()) == 1)
            self.assertEqual(self.docman._search()[0]["ns"], dest_mapping[ns])
            bad = [d for d in self.docman._search() if d["ns"] == ns]
            self.assertEqual(len(bad), 0)

            # test update
            self.primary_conn[db][coll].update(
                {"_id": 1},
                {"$set": {"weakness": "kryptonite"}}
            )

            def update_complete():
                docs = self.docman._search()
                for d in docs:
                    if d.get("weakness") == "kryptonite":
                        return True
                    return False
            assert_soon(update_complete)
            self.assertEqual(self.docman._search()[0]["ns"], dest_mapping[ns])
            bad = [d for d in self.docman._search() if d["ns"] == ns]
            self.assertEqual(len(bad), 0)

            # test delete
            self.primary_conn[db][coll].remove({"_id": 1})
            assert_soon(lambda: len(self.docman._search()) == 0)
            bad = [d for d in self.docman._search()
                   if d["ns"] == dest_mapping[ns]]
            self.assertEqual(len(bad), 0)

            # cleanup
            self.primary_conn[db][coll].remove()
            self.opman.doc_managers[0]._delete()

        # doc not in namespace set
        for ns in phony_ns:
            db, coll = ns.split(".", 1)

            # test insert
            self.primary_conn[db][coll].insert(base_doc)
            time.sleep(1)
            self.assertEqual(len(self.docman._search()), 0)
            # test update
            self.primary_conn[db][coll].update(
                {"_id": 1},
                {"$set": {"weakness": "kryptonite"}}
            )
            time.sleep(1)
            self.assertEqual(len(self.docman._search()), 0)

    def test_many_targets(self):
        """Test that one OplogThread is capable of replicating to more than
        one target.
        """
        doc_managers = [DocManager(), DocManager(), DocManager()]
        self.opman.doc_managers = doc_managers

        # start replicating
        self.opman.start()
        self.primary_conn["test"]["test"].insert({
            "name": "kermit",
            "color": "green"
        })
        self.primary_conn["test"]["test"].insert({
            "name": "elmo",
            "color": "firetruck red"
        })

        assert_soon(
            lambda: sum(len(d._search()) for d in doc_managers) == 6,
            "OplogThread should be able to replicate to multiple targets"
        )

        self.primary_conn["test"]["test"].remove({"name": "elmo"})

        assert_soon(
            lambda: sum(len(d._search()) for d in doc_managers) == 3,
            "OplogThread should be able to replicate to multiple targets"
        )
        for d in doc_managers:
            self.assertEqual(d._search()[0]["name"], "kermit")


if __name__ == '__main__':
    unittest.main()
