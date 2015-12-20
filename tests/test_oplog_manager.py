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

import itertools
import re
import sys
import time

import bson
import pymongo

sys.path[0:0] = [""]

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from tests import unittest
from tests.setup_cluster import ReplicaSet
from tests.util import assert_soon


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """

    def setUp(self):
        self.repl_set = ReplicaSet().start()
        self.primary_conn = self.repl_set.client()
        self.oplog_coll = self.primary_conn.local['oplog.rs']
        self.opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict()
        )

    def tearDown(self):
        try:
            self.opman.join()
        except RuntimeError:
            pass                # OplogThread may not have been started
        self.primary_conn.close()
        self.repl_set.stop()

    def test_get_oplog_cursor(self):
        '''Test the get_oplog_cursor method'''

        # timestamp is None - all oplog entries are returned.
        cursor = self.opman.get_oplog_cursor(None)
        self.assertEqual(cursor.count(),
                         self.primary_conn["local"]["oplog.rs"].count())

        # earliest entry is the only one at/after timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.primary_conn["test"]["test"].insert(doc)
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
            {'op': {'$ne': 'n'},
             'ns': {'$not': re.compile(r'\.(system|\$cmd)')}},
            sort=[("ts", pymongo.ASCENDING)]
        )

        # initial insert + 1000 more inserts
        self.assertEqual(oplog_cursor.count(), 1 + 1000)
        pivot = oplog_cursor.skip(400).limit(1)[0]

        goc_cursor = self.opman.get_oplog_cursor(pivot["ts"])
        self.assertEqual(goc_cursor.count(), 1 + 1000 - 400)

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
        docs.sort(key=lambda doc: doc['a'])

        self.assertEqual(len(docs), 90)
        expected_a = itertools.chain(range(0, 50), range(60, 100))
        for doc, correct_a in zip(docs, expected_a):
            self.assertEqual(doc['a'], correct_a)

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
        self.opman.oplog = self.primary_conn["test"]["emptycollection"]
        self.opman.collection_dump = False
        self.assertTrue(all(doc['op'] == 'n'
                            for doc in self.opman.init_cursor()[0]))
        self.assertEqual(self.opman.checkpoint, None)

        # No last checkpoint, empty collections, nothing in oplog
        self.opman.collection_dump = True
        cursor, cursor_len = self.opman.init_cursor()
        self.assertEqual(cursor, None)
        self.assertEqual(cursor_len, 0)
        self.assertEqual(self.opman.checkpoint, None)

        # No last checkpoint, empty collections, something in oplog
        self.opman.oplog = self.primary_conn['local']['oplog.rs']
        collection = self.primary_conn["test"]["test"]
        collection.insert({"i": 1})
        collection.remove({"i": 1})
        time.sleep(3)
        last_ts = self.opman.get_last_oplog_timestamp()
        cursor, cursor_len = self.opman.init_cursor()
        self.assertEqual(cursor_len, 0)
        self.assertEqual(self.opman.checkpoint, last_ts)
        with self.opman.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman.oplog)], last_ts)

        # No last checkpoint, no collection dump, something in oplog
        self.opman.oplog_progress = LockingDict()
        self.opman.collection_dump = False
        collection.insert({"i": 2})
        last_ts = self.opman.get_last_oplog_timestamp()
        cursor, cursor_len = self.opman.init_cursor()
        for i in range(cursor_len - 1):
            next(cursor)
        self.assertEqual(next(cursor)['o']['i'], 2)
        self.assertEqual(self.opman.checkpoint, last_ts)

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
        cursor, cursor_len = self.opman.init_cursor()
        self.assertEqual(next(cursor)["ts"], entry[1]["ts"])
        self.assertEqual(self.opman.checkpoint, entry[0]["ts"])
        with self.opman.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman.oplog)],
                             entry[0]["ts"])

        # Last checkpoint is behind
        progress = LockingDict()
        progress.get_dict()[str(self.opman.oplog)] = bson.Timestamp(1, 0)
        self.opman.oplog_progress = progress
        self.opman.checkpoint = None
        cursor, cursor_len = self.opman.init_cursor()
        self.assertEqual(cursor_len, 0)
        self.assertEqual(cursor, None)
        self.assertIsNotNone(self.opman.checkpoint)

    def test_filter_fields(self):
        docman = self.opman.doc_managers[0]
        conn = self.opman.primary_client

        include_fields = ["a", "b", "c"]
        exclude_fields = ["d", "e", "f"]

        # Set fields to care about
        self.opman.fields = {'include': include_fields}
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

    def test_filter_fields_exclude(self):
        docman = self.opman.doc_managers[0]
        conn = self.opman.primary_client

        exclude_fields = ["d", "e", "f"]

        # Set fields to care about
        self.opman.fields = {'exclude': exclude_fields}
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
        for exc in exclude_fields:
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
        self.opman.fields = {'include': ['a', 'b']}
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0, 'a': 1, 'b': 2})

        # Case 2: insert op, exclude fields provided
        self.opman.fields = {'exclude': ['a', 'b']}
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0, 'c': 3})

        # Case 3: insert op, fields provided, doc becomes empty except for _id
        self.opman.fields = {'include': ['d', 'e', 'f']}
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0})

        # Case 4: insert op, exlude fields provided, full doc retained
        self.opman.fields = {'exclude': ['d', 'e', 'f']}
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0, 'a': 1, 'b': 2, 'c': 3})

        # Case 5: update op, no fields provided
        self.opman.fields = None
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, update_op())

        # Case 6: update op, fields provided
        self.opman.fields = {'include': ['a', 'c']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('b', filtered['o']['$set'])
        self.assertIn('a', filtered['o']['$set'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])

        # Case 7: update op, exclude fields provided
        self.opman.fields = {'exclude': ['a', 'c']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('a', filtered['o']['$set'])
        self.assertNotIn('c', filtered['o']['$set'])
        self.assertIn('b', filtered['o']['$set'])
        self.assertNotIn('$unset', filtered['o'])

        # Case 8: update op, fields provided, empty $set
        self.opman.fields = {'include': ['c']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$set', filtered['o'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])

        # Case 9: update op, exlcude fields provided, empty $unset
        self.opman.fields = {'exclude': ['c']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$unset', filtered['o'])
        self.assertEqual(filtered['o']['$set'], update_op()['o']['$set'])

        # Case 10: update op, fields provided, empty $unset
        self.opman.fields = {'include': ['a', 'b']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$unset', filtered['o'])
        self.assertEqual(filtered['o']['$set'], update_op()['o']['$set'])

        # Case 11: update op, exclude fields provided, empty $set
        self.opman.fields = {'exclude': ['a', 'b']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$set', filtered['o'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])

        # Case 12: update op, fields provided, entry is nullified
        self.opman.fields = {'include': ['d', 'e', 'f']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, None)

        # Case 13: update op, exclude fields provided, entry is nullified
        self.opman.fields = {'exclude': ['a', 'b', 'c']}
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, None)

        # Case 14: update op, fields provided, replacement
        self.opman.fields = {'include': ['a', 'b', 'c']}
        filtered = self.opman.filter_oplog_entry({
            'op': 'u',
            'o': {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        })
        self.assertEqual(
            filtered, {'op': 'u', 'o': {'a': 1, 'b': 2, 'c': 3}})

        # Case 15: update op, fields provided, replacement
        self.opman.fields = {'exclude': ['a', 'b', 'c']}
        filtered = self.opman.filter_oplog_entry({
            'op': 'u',
            'o': {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        })
        self.assertEqual(
            filtered, {'op': 'u', 'o': {'d': 4}})

if __name__ == '__main__':
    unittest.main()
