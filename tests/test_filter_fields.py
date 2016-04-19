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

"""Test include and exclude fields
"""
import sys

sys.path[0:0] = [""]

from mongo_connector import errors
from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.test_utils import ReplicaSet, assert_soon, close_client
from tests import unittest


class TestFilterFields(unittest.TestCase):

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
        self.primary_conn.drop_database("test")
        close_client(self.primary_conn)
        self.repl_set.stop()

    def _check_fields(self, opman, fields, exclude_fields, projection):
        if fields:
            self.assertEqual(sorted(opman.fields), sorted(fields))
            self.assertEqual(opman._fields, set(fields))
        else:
            self.assertEqual(opman.fields, None)
            self.assertEqual(opman._fields, set([]))
        if exclude_fields:
            self.assertEqual(sorted(opman.exclude_fields), sorted(exclude_fields))
            self.assertEqual(opman._exclude_fields, set(exclude_fields))
        else:
            self.assertEqual(opman.exclude_fields, None)
            self.assertEqual(opman._exclude_fields, set([]))

        self.assertEqual(opman._projection, projection)

    def test_filter_fields(self):
        docman = self.opman.doc_managers[0]
        conn = self.opman.primary_client

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
        db.insert_one(doc)
        assert_soon(lambda: db.count() == 1)
        self.opman.dump_collection()

        result = docman._search()[0]
        keys = result.keys()
        for inc, exc in zip(include_fields, exclude_fields):
            self.assertIn(inc, keys)
            self.assertNotIn(exc, keys)

    def test_filter_exclude_oplog_entry(self):
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
        self.opman.exclude_fields = None
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered, insert_op())
        self.assertEqual(None, self.opman._projection)

        # Case 1: insert op, fields provided
        self.opman.exclude_fields = ['c']
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0, 'a': 1, 'b': 2})
        self.assertEqual({'c': 0}, self.opman._projection)

        # Case 2: insert op, fields provided, doc becomes empty except for _id
        self.opman.exclude_fields = ['a', 'b', 'c']
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0})
        self.assertEqual({'a': 0, 'b': 0, 'c': 0}, self.opman._projection)

        # Case 3: update op, no fields provided
        self.opman.exclude_fields = None
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, update_op())
        self.assertEqual(None, self.opman._projection)

        # Case 4: update op, fields provided
        self.opman.exclude_fields = ['b']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('b', filtered['o']['$set'])
        self.assertIn('a', filtered['o']['$set'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])
        self.assertEqual({'b': 0}, self.opman._projection)

        # Case 5: update op, fields provided, empty $set
        self.opman.exclude_fields = ['a', 'b']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$set', filtered['o'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])
        self.assertEqual({'a': 0, 'b': 0}, self.opman._projection)

        # Case 6: update op, fields provided, empty $unset
        self.opman.exclude_fields = ['c']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$unset', filtered['o'])
        self.assertEqual(filtered['o']['$set'], update_op()['o']['$set'])
        self.assertEqual({'c': 0}, self.opman._projection)

        # Case 7: update op, fields provided, entry is nullified
        self.opman.exclude_fields = ['a', 'b', 'c']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, None)
        self.assertEqual({'a': 0, 'b': 0, 'c': 0}, self.opman._projection)

        # Case 8: update op, fields provided, replacement
        self.opman.exclude_fields = ['d', 'e', 'f']
        filtered = self.opman.filter_oplog_entry({
            'op': 'u',
            'o': {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        })
        self.assertEqual(
            filtered, {'op': 'u', 'o': {'a': 1, 'b': 2, 'c': 3}})
        self.assertEqual({'d': 0, 'e': 0, 'f': 0}, self.opman._projection)

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
        self.assertEqual(None, self.opman._projection)

        # Case 1: insert op, fields provided
        self.opman.fields = ['a', 'b']
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0, 'a': 1, 'b': 2})
        self.assertEqual({'_id': 1, 'a': 1, 'b': 1}, self.opman._projection)

        # Case 2: insert op, fields provided, doc becomes empty except for _id
        self.opman.fields = ['d', 'e', 'f']
        filtered = self.opman.filter_oplog_entry(insert_op())
        self.assertEqual(filtered['o'], {'_id': 0})
        self.assertEqual({'_id': 1, 'd': 1, 'e': 1, 'f': 1},
                         self.opman._projection)

        # Case 3: update op, no fields provided
        self.opman.fields = None
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, update_op())
        self.assertEqual(None, self.opman._projection)

        # Case 4: update op, fields provided
        self.opman.fields = ['a', 'c']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('b', filtered['o']['$set'])
        self.assertIn('a', filtered['o']['$set'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])
        self.assertEqual({'_id': 1, 'a': 1, 'c': 1}, self.opman._projection)

        # Case 5: update op, fields provided, empty $set
        self.opman.fields = ['c']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$set', filtered['o'])
        self.assertEqual(filtered['o']['$unset'], update_op()['o']['$unset'])
        self.assertEqual({'_id': 1, 'c': 1}, self.opman._projection)

        # Case 6: update op, fields provided, empty $unset
        self.opman.fields = ['a', 'b']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertNotIn('$unset', filtered['o'])
        self.assertEqual(filtered['o']['$set'], update_op()['o']['$set'])
        self.assertEqual({'_id': 1, 'a': 1, 'b': 1}, self.opman._projection)

        # Case 7: update op, fields provided, entry is nullified
        self.opman.fields = ['d', 'e', 'f']
        filtered = self.opman.filter_oplog_entry(update_op())
        self.assertEqual(filtered, None)
        self.assertEqual({'_id': 1, 'd': 1, 'e': 1, 'f': 1},
                         self.opman._projection)

        # Case 8: update op, fields provided, replacement
        self.opman.fields = ['a', 'b', 'c']
        filtered = self.opman.filter_oplog_entry({
            'op': 'u',
            'o': {'a': 1, 'b': 2, 'c': 3, 'd': 4}
        })
        self.assertEqual(
            filtered, {'op': 'u', 'o': {'a': 1, 'b': 2, 'c': 3}})
        self.assertEqual({'_id': 1, 'a': 1, 'b': 1, 'c': 1},
                         self.opman._projection)

    def test_exclude_fields_constructor(self):
        # Test with the "_id" field in exclude_fields
        exclude_fields = ["_id", "title", "content", "author"]
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            exclude_fields=exclude_fields
        )
        exclude_fields.remove('_id')
        self._check_fields(opman, [], exclude_fields,
                           dict((f, 0) for f in exclude_fields))
        extra_fields = exclude_fields + ['extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in ['extra1', 'extra2']), filtered)

        # Test without "_id" field included in exclude_fields
        exclude_fields = ["title", "content", "author"]
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            exclude_fields=exclude_fields
        )
        self._check_fields(opman, [], exclude_fields,
                           dict((f, 0) for f in exclude_fields))
        extra_fields = extra_fields + ['extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual({'extra1': 1, 'extra2': 1}, filtered)

        # Test with only "_id" field in exclude_fields
        exclude_fields = ["_id"]
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            exclude_fields=exclude_fields
        )
        self._check_fields(opman, [], [], None)
        extra_fields = exclude_fields + ['extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in extra_fields), filtered)

        # Test with nothing set for exclude_fields
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            exclude_fields=None
        )
        self._check_fields(opman, [], [], None)
        extra_fields = ['_id', 'extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in extra_fields), filtered)

    def test_fields_constructor(self):
        # Test with "_id" field in constructor
        fields = ["_id", "title", "content", "author"]
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            fields=fields
        )
        self._check_fields(opman, fields, [],
                           dict((f, 1) for f in fields))
        extra_fields = fields + ['extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in fields), filtered)

        # Test without "_id" field in constructor
        fields = ["title", "content", "author"]
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            fields = fields
        )
        fields.append('_id')
        self._check_fields(opman, fields, [],
                           dict((f, 1) for f in fields))
        extra_fields = fields + ['extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in fields), filtered)

        # Test with only "_id" field
        fields = ["_id"]
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            fields = fields
        )
        self._check_fields(opman, fields, [],
                           dict((f, 1) for f in fields))
        extra_fields = fields + ['extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual({'_id': 1}, filtered)

        # Test with no fields set
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
        )
        self._check_fields(opman, [], [], None)
        extra_fields = ['_id', 'extra1', 'extra2']
        filtered = opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in extra_fields), filtered)

    def test_exclude_fields_attr(self):
        # Test with the "_id" field in exclude_fields.
        exclude_fields = ["_id", "title", "content", "author"]
        exclude_fields.remove('_id')
        self.opman.exclude_fields = exclude_fields
        self._check_fields(self.opman, [], exclude_fields,
                           dict((f, 0) for f in exclude_fields))
        extra_fields = exclude_fields + ['extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in ['extra1', 'extra2']), filtered)

        # Test without "_id" field included in exclude_fields
        exclude_fields = ["title", "content", "author"]
        self.opman.exclude_fields = exclude_fields
        self._check_fields(self.opman, [], exclude_fields,
                           dict((f, 0) for f in exclude_fields))
        extra_fields = extra_fields + ['extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual({'extra1': 1, 'extra2': 1}, filtered)

        # Test with only "_id" field in exclude_fields
        exclude_fields = ["_id"]
        self.opman.exclude_fields = exclude_fields
        self._check_fields(self.opman, [], [], None)
        extra_fields = exclude_fields + ['extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in extra_fields), filtered)

        # Test with nothing set for exclude_fields
        self.opman.exclude_fields = None
        self._check_fields(self.opman, [], [], None)
        extra_fields = ['_id', 'extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in extra_fields), filtered)

    def test_fields_attr(self):
        # Test with "_id" field included in fields
        fields = ["_id", "title", "content", "author"]
        self.opman.fields = fields
        self._check_fields(self.opman, fields, [], dict((f, 1) for f in fields))
        extra_fields = fields + ['extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in fields), filtered)

        # Test without "_id" field included in fields
        fields = ["title", "content", "author"]
        self.opman.fields = fields
        fields.append('_id')
        self._check_fields(self.opman, fields, [], dict((f, 1) for f in fields))
        extra_fields = fields + ['extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in fields), filtered)

        # Test with only "_id" field
        fields = ["_id"]
        self.opman.fields = fields
        self._check_fields(self.opman, fields, [], dict((f, 1) for f in fields))
        extra_fields = fields + ['extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual({'_id': 1}, filtered)

        # Test with no fields set
        self.opman.fields = None
        self._check_fields(self.opman, [], [], None)
        extra_fields = ['_id', 'extra1', 'extra2']
        filtered = self.opman.filter_oplog_entry(
            {'op': 'i',
             'o': dict((f, 1) for f in extra_fields)})['o']
        self.assertEqual(dict((f, 1) for f in extra_fields), filtered)

    def test_nested_fields(self):
        def check_nested(document, fields, filtered_document):
            self.opman.fields = fields
            fields.append('_id')
            self.assertEqual(set(fields), self.opman._fields)
            self.assertEqual(sorted(fields), sorted(self.opman.fields))
            filtered_result = self.opman.filter_oplog_entry(
                {'op': 'i',
                 'o': document})['o']
            self.assertEqual(filtered_result, filtered_document)

        document = {'name': 'Han Solo', 'a': {'b': {}}}
        fields = ['name', 'a.b.c']
        filtered_document = {'name': 'Han Solo'}
        check_nested(document, fields, filtered_document)

        document = {'a': {'b': {'c': 2, 'e': 3}, 'e': 5},
                    'b': 2,
                    'c': {'g': 1}}
        fields = ['a.b.c', 'a.e']
        filtered_document = {'a': {'b': {'c': 2}, 'e': 5}}
        check_nested(document, fields, filtered_document)

        document = {'a': {'b': {'c': 2, 'e': 3}, 'e': 5},
                    'b': 2,
                    'c': {'g': 1},
                    '_id': 1}
        fields = ['a.b.c', 'a.e']
        filtered_document = {'a': {'b': {'c': 2}, 'e': 5}, '_id': 1}
        check_nested(document, fields, filtered_document)

        document = {'a': {'b': {'c': {'d': 1}}}, '-a': {'-b': {'-c': 2}}}
        fields = ['a.b', '-a']
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)

        document = {'a': {'b': {'c': {'d': 1}}}, '-a': {'-b': {'-c': 2}}}
        fields = ['a', '-a.-b']
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)
        document = {'a': {'b': {'c': {'d': 1}}}, '-a': {'-b': {'-c': 2}},
                    '_id': 1}

        fields = ['a.b', '-a']
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)
        fields = ['a', '-a.-b']
        check_nested(document, fields, filtered_document)

        document = {'test': 1}
        fields = ['doesnt_exist']
        filtered_document = {}
        check_nested(document, fields, filtered_document)

        document = {'a': {'b': 1}, 'b': {'a': 1}}
        fields = ['a.b', 'b.a']
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)

        document = {'a': {'b': {'a': {'b': 1}}}, 'c': {'a': {'b': 1}}}
        fields = ['a.b']
        filtered_document = {'a': {'b': {'a': {'b': 1}}}}
        check_nested(document, fields, filtered_document)

        document = {'name': 'anna', 'name_of_cat': 'pushkin'}
        fields = ['name']
        filtered_document = {'name': 'anna'}
        check_nested(document, fields, filtered_document)

    def test_nested_exclude_fields(self):
        def check_nested(document, exclude_fields, filtered_document):
            self.opman.exclude_fields = exclude_fields
            if '_id' in exclude_fields:
                exclude_fields.remove('_id')
            self.assertEqual(set(exclude_fields), self.opman._exclude_fields)
            self.assertEqual(sorted(exclude_fields),
                             sorted(self.opman.exclude_fields))
            filtered_result = self.opman.filter_oplog_entry(
                {'op': 'i',
                 'o': document})['o']
            self.assertEqual(filtered_result, filtered_document)

        document = {'a': {'b': {'c': {'d': 0, 'e': 1}}}}
        exclude_fields = ['a.b.c.d']
        filtered_document = {'a': {'b': {'c': {'e': 1}}}}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'c': {'-a': 0, 'd': {'e': {'f': 1}}}}}}
        exclude_fields = ['a.b.c.d.e.f']
        filtered_document = {'a': {'b': {'c': {'-a': 0}}}}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': 1}
        exclude_fields = ['a']
        filtered_document = {}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'c': 2, 'e': 3}, 'e': 5},
                    'b': 2,
                    'c': {'g': 1}}
        exclude_fields = ['a.b.c', 'a.e']
        filtered_document = {'a': {'b': {'e': 3}},
                             'b': 2,
                             'c': {'g': 1}}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'c': 2, 'e': 3}, 'e': 5},
                    'b': 2,
                    'c': {'g': 1},
                    '_id': 1}
        exclude_fields = ['a.b.c', 'a.e', '_id']
        filtered_document = {'a': {'b': {'e': 3}},
                             'b': 2, 'c': {'g': 1},
                             '_id': 1}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'c': {'d': 1}}},
                    '-a': {'-b': {'-c': 2}}}
        exclude_fields = ['a.b', '-a']
        filtered_document = {}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'c': {'d': 1}}},
                    '-a': {'-b': {'-c': 2}}}
        exclude_fields = ['a', '-a.-b']
        filtered_document = {}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'c': {'d': 1}}},
                    '-a': {'-b': {'-c': 2}},
                    '_id': 1}
        exclude_fields = ['a.b', '-a']
        filtered_document = {'_id': 1}
        check_nested(document, exclude_fields, filtered_document)

        document = {'test': 1}
        exclude_fields = ['doesnt_exist']
        filtered_document = document.copy()
        check_nested(document, exclude_fields, filtered_document)

        document = {'test': 1}
        exclude_fields = ['test.doesnt_exist']
        filtered_document = document.copy()
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': 1}, 'b': {'a': 1}}
        exclude_fields = ['a.b', 'b.a']
        filtered_document = {}
        check_nested(document, exclude_fields, filtered_document)

        document = {'a': {'b': {'a': {'b': 1}}}, 'c': {'a': {'b': 1}}}
        exclude_fields = ['a.b']
        filtered_document = {'c': {'a': {'b': 1}}}
        check_nested(document, exclude_fields, filtered_document)

        document = {'name': 'anna', 'name_of_cat': 'pushkin'}
        exclude_fields = ['name']
        filtered_document = {'name_of_cat': 'pushkin'}
        check_nested(document, exclude_fields, filtered_document)

    def test_fields_and_exclude(self):
        fields = ['a', 'b', 'c', '_id']
        exclude_fields = ['x', 'y', 'z']

        # Test setting both to None in constructor
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            fields=None,
            exclude_fields=None
        )
        self._check_fields(opman, [], [], None)
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            fields=None,
            exclude_fields=exclude_fields
        )
        self._check_fields(opman, [], exclude_fields,
                           dict((f, 0) for f in exclude_fields))
        # Test setting fields when exclude_fields is set
        self.assertRaises(
            errors.InvalidConfiguration, setattr, opman, "fields", fields)
        self.assertRaises(
            errors.InvalidConfiguration, setattr, opman, "fields", None)
        opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            exclude_fields=None,
            fields=fields
        )
        self._check_fields(opman, fields, [], dict((f, 1) for f in fields))
        self.assertRaises(errors.InvalidConfiguration, setattr, opman,
                          "exclude_fields", exclude_fields)
        self.assertRaises(errors.InvalidConfiguration, setattr, opman,
                          "exclude_fields", None)
        self.assertRaises(
            errors.InvalidConfiguration, OplogThread,
            self.primary_conn,
            (DocManager(),),
            LockingDict(),
            fields=fields,
            exclude_fields=exclude_fields)
