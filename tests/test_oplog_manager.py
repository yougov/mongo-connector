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
from tests import mongo_host
from tests.setup_cluster import (start_replica_set,
                                 kill_replica_set)
from tests.util import assert_soon


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """

    def setUp(self):
        _, _, self.primary_p = start_replica_set('test-oplog-manager')
        self.primary_conn = pymongo.MongoClient(mongo_host, self.primary_p)
        self.oplog_coll = self.primary_conn.local['oplog.rs']
        self.opman = OplogThread(
            primary_conn=self.primary_conn,
            main_address='%s:%d' % (mongo_host, self.primary_p),
            oplog_coll=self.oplog_coll,
            is_sharded=False,
            doc_manager=DocManager(),
            oplog_progress_dict=LockingDict(),
            namespace_set=None,
            auth_key=None,
            auth_username=None,
            repl_set='test-oplog-manager'
        )

    def tearDown(self):
        try:
            self.opman.join()
        except RuntimeError:
            pass                # OplogThread may not have been started
        self.primary_conn.close()
        kill_replica_set('test-oplog-manager')

    def test_get_oplog_cursor(self):
        '''Test the get_oplog_cursor method'''

        # Trivial case: timestamp is None
        self.assertEqual(self.opman.get_oplog_cursor(None), None)

        # earliest entry is after given timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.primary_conn["test"]["test"].insert(doc)
        self.assertEqual(self.opman.get_oplog_cursor(
            bson.Timestamp(1, 0)), None)

        # earliest entry is the only one at/after timestamp
        latest_timestamp = self.opman.get_last_oplog_timestamp()
        cursor = self.opman.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count(), 1)
        next_entry_id = next(cursor)['o']['_id']
        retrieved = self.primary_conn.test.test.find_one(next_entry_id)
        self.assertEqual(retrieved, doc)

        # many entries before and after timestamp
        self.primary_conn["test"]["test"].insert(
            {"i": i} for i in range(2, 1002))
        oplog_cursor = self.oplog_coll.find(
            sort=[("ts", pymongo.ASCENDING)]
        )

        # startup + insert + 1000 inserts
        self.assertEqual(oplog_cursor.count(), 2 + 1000)
        pivot = oplog_cursor.skip(400).limit(1)[0]

        goc_cursor = self.opman.get_oplog_cursor(pivot["ts"])
        self.assertEqual(goc_cursor.count(), 2 + 1000 - 400)

        # get_oplog_cursor fast-forwards *one doc beyond* the given timestamp
        doc = self.primary_conn["test"]["test"].find_one(
            {"_id": next(goc_cursor)["o"]["_id"]})
        retrieved = self.primary_conn.test.test.find_one(pivot['o']['_id'])
        self.assertEqual(doc["i"], retrieved["i"] + 1)

    def test_get_last_oplog_timestamp(self):
        """Test the get_last_oplog_timestamp method"""

        # "empty" the oplog
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        self.assertEqual(self.opman.get_last_oplog_timestamp(), None)

        # Test non-empty oplog
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.primary_conn["test"]["test"].insert({
                "i": i + 500
            })
        oplog = self.primary_conn["local"]["oplog.rs"]
        oplog = oplog.find().sort("$natural", pymongo.DESCENDING).limit(1)[0]
        self.assertEqual(self.opman.get_last_oplog_timestamp(),
                         oplog["ts"])

    def test_dump_collection(self):
        """Test the dump_collection method

        Cases:

        1. empty oplog
        2. non-empty oplog
        """

        # Test with empty oplog
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        last_ts = self.opman.dump_collection()
        self.assertEqual(last_ts, None)

        # Test with non-empty oplog
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.primary_conn["test"]["test"].insert({
                "i": i + 500
            })
        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        self.assertEqual(len(self.opman.doc_managers[0]._search()), 1000)

    def test_dump_collection_with_error(self):
        """Test the dump_collection method with invalid documents.

        Cases:

        1. non-empty oplog, continue_on_error=True, invalid documents
        """

        # non-empty oplog, continue_on_error=True, invalid documents
        self.opman.continue_on_error = True
        self.opman.oplog = self.primary_conn["local"]["oplog.rs"]

        docs = [{'a': i} for i in range(100)]
        for i in range(50, 60):
            docs[i]['_upsert_exception'] = True
        self.primary_conn['test']['test'].insert(docs)

        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(last_ts, self.opman.dump_collection())
        docs = self.opman.doc_managers[0]._search()
        docs.sort()
        
        self.assertEqual(len(docs), 90)
        for doc, correct_a in zip(docs, range(0, 50) + range(60, 100)):
            self.assertEquals(doc['a'], correct_a)

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
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        self.opman.collection_dump = False
        self.assertEqual(self.opman.init_cursor(), None)
        self.assertEqual(self.opman.checkpoint, None)

        # No last checkpoint, empty collections, nothing in oplog
        self.opman.collection_dump = True
        self.assertEqual(self.opman.init_cursor(), None)
        self.assertEqual(self.opman.checkpoint, None)

        # No last checkpoint, empty collections, something in oplog
        self.opman.oplog = self.primary_conn['local']['oplog.rs']
        collection = self.primary_conn["test"]["test"]
        collection.insert({"i": 1})
        collection.remove({"i": 1})
        time.sleep(3)
        last_ts = self.opman.get_last_oplog_timestamp()
        self.assertEqual(next(self.opman.init_cursor())["ts"], last_ts)
        self.assertEqual(self.opman.checkpoint, last_ts)
        with self.opman.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman.oplog)], last_ts)

        # No last checkpoint, non-empty collections, stuff in oplog
        self.opman.oplog_progress = LockingDict()
        self.assertEqual(next(self.opman.init_cursor())["ts"], last_ts)
        self.assertEqual(self.opman.checkpoint, last_ts)
        with self.opman.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman.oplog)], last_ts)

        # Last checkpoint exists
        progress = LockingDict()
        self.opman.oplog_progress = progress
        for i in range(1000):
            collection.insert({"i": i + 500})
        entry = list(
            self.primary_conn["local"]["oplog.rs"].find(skip=200, limit=2))
        progress.get_dict()[str(self.opman.oplog)] = entry[0]["ts"]
        self.opman.oplog_progress = progress
        self.opman.checkpoint = None
        cursor = self.opman.init_cursor()
        self.assertEqual(entry[1]["ts"], next(cursor)["ts"])
        self.assertEqual(self.opman.checkpoint, entry[0]["ts"])
        with self.opman.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman.oplog)],
                             entry[0]["ts"])

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
        docman = self.opman.doc_managers[0]
        # start replicating
        self.opman.start()

        base_doc = {"_id": 1, "name": "superman"}

        # doc in namespace set
        for ns in source_ns:
            db, coll = ns.split(".", 1)

            # test insert
            self.primary_conn[db][coll].insert(base_doc)

            assert_soon(lambda: len(docman._search()) == 1)
            self.assertEqual(docman._search()[0]["ns"], dest_mapping[ns])
            bad = [d for d in docman._search() if d["ns"] == ns]
            self.assertEqual(len(bad), 0)

            # test update
            self.primary_conn[db][coll].update(
                {"_id": 1},
                {"$set": {"weakness": "kryptonite"}}
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
            self.primary_conn[db][coll].remove({"_id": 1})
            assert_soon(lambda: len(docman._search()) == 0)
            bad = [d for d in docman._search()
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
            self.assertEqual(len(docman._search()), 0)
            # test update
            self.primary_conn[db][coll].update(
                {"_id": 1},
                {"$set": {"weakness": "kryptonite"}}
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

    def test_filter_oplog_entry(self):
        # Test oplog entries: these are callables, since
        # filter_oplog_entry modifies the oplog entry in-place
        insert_op = lambda: {
            "op": "i",
            "o": {
                "_id": 0,
                "a": 1,
                "b": 2,
                "c": 3
            }
        }
        update_op = lambda: {
            "op": "u",
            "o": {
                "$set": {
                    "a": 4,
                    "b": 5
                },
                "$unset": {
                    "c": True
                }
            },
            "o2": {
                "_id": 1
            }
        }

        # Case 0: insert op, no fields provided
        self.opman.fields = None
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered, insert_op())

        # Case 1: insert op, fields provided
        self.opman.fields = ['a', 'b']
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0, 'a': 1, 'b': 2})

        # Case 2: insert op, fields provided, doc becomes empty except for _id
        self.opman.fields = ['d', 'e', 'f']
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0})

        # Case 3: update op, no fields provided
        self.opman.fields = None
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, update_op())

        # Case 4: update op, fields provided
        self.opman.fields = ['a', 'c']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('b', filtered['o']['$set'])
        self.assertIn('a', filtered['o']['$set'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])

        # Case 5: update op, fields provided, empty $set
        self.opman.fields = ['c']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$set', filtered['o'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])

        # Case 6: update op, fields provided, empty $unset
        self.opman.fields = ['a', 'b']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$unset', filtered['o'])
        self.assertEqual(filtered['o']['$set'], update_op()['o']['$set'])

        # Case 7: update op, fields provided, entry is nullified
        self.opman.fields = ['d', 'e', 'f']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, None)

if __name__ == '__main__':
    unittest.main()
