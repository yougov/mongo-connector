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

sys.path[0:0] = [""]  # noqa

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.namespace_config import NamespaceConfig
from mongo_connector.test_utils import assert_soon, close_client, ReplicaSetSingle
from tests import unittest


class TestFilterFields(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repl_set = ReplicaSetSingle().start()
        cls.primary_conn = cls.repl_set.client()
        cls.oplog_coll = cls.primary_conn.local["oplog.rs"]

    @classmethod
    def tearDownClass(cls):
        cls.primary_conn.drop_database("test")
        close_client(cls.primary_conn)
        cls.repl_set.stop()

    def setUp(self):
        self.namespace_config = NamespaceConfig()
        self.opman = OplogThread(
            primary_client=self.primary_conn,
            doc_managers=(DocManager(),),
            oplog_progress_dict=LockingDict(),
            namespace_config=self.namespace_config,
        )

    def tearDown(self):
        try:
            self.opman.join()
        except RuntimeError:
            # OplogThread may not have been started
            pass

    def reset_include_fields(self, fields):
        self.opman.namespace_config = NamespaceConfig(include_fields=fields)

    def reset_exclude_fields(self, fields):
        self.opman.namespace_config = NamespaceConfig(exclude_fields=fields)

    def test_filter_fields(self):
        docman = self.opman.doc_managers[0]
        conn = self.opman.primary_client

        include_fields = ["a", "b", "c"]
        exclude_fields = ["d", "e", "f"]

        # Set fields to care about
        self.reset_include_fields(include_fields)
        # Documents have more than just these fields
        doc = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "_id": 1}
        db = conn["test"]["test"]
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
        def insert_op():
            return {"op": "i", "o": {"_id": 0, "a": 1, "b": 2, "c": 3}}

        def update_op():
            return {
                "op": "u",
                "o": {"$set": {"a": 4, "b": 5}, "$unset": {"c": True}},
                "o2": {"_id": 1},
            }

        def filter_doc(document, fields):
            if fields and "_id" in fields:
                fields.remove("_id")
            return self.opman.filter_oplog_entry(document, exclude_fields=fields)

        # Case 0: insert op, no fields provided
        filtered = filter_doc(insert_op(), None)
        self.assertEqual(filtered, insert_op())

        # Case 1: insert op, fields provided
        filtered = filter_doc(insert_op(), ["c"])
        self.assertEqual(filtered["o"], {"_id": 0, "a": 1, "b": 2})

        # Case 2: insert op, fields provided, doc becomes empty except for _id
        filtered = filter_doc(insert_op(), ["a", "b", "c"])
        self.assertEqual(filtered["o"], {"_id": 0})

        # Case 3: update op, no fields provided
        filtered = filter_doc(update_op(), None)
        self.assertEqual(filtered, update_op())

        # Case 4: update op, fields provided
        filtered = filter_doc(update_op(), ["b"])
        self.assertNotIn("b", filtered["o"]["$set"])
        self.assertIn("a", filtered["o"]["$set"])
        self.assertEqual(filtered["o"]["$unset"], update_op()["o"]["$unset"])

        # Case 5: update op, fields provided, empty $set
        filtered = filter_doc(update_op(), ["a", "b"])
        self.assertNotIn("$set", filtered["o"])
        self.assertEqual(filtered["o"]["$unset"], update_op()["o"]["$unset"])

        # Case 6: update op, fields provided, empty $unset
        filtered = filter_doc(update_op(), ["c"])
        self.assertNotIn("$unset", filtered["o"])
        self.assertEqual(filtered["o"]["$set"], update_op()["o"]["$set"])

        # Case 7: update op, fields provided, entry is nullified
        filtered = filter_doc(update_op(), ["a", "b", "c"])
        self.assertEqual(filtered, None)

        # Case 8: update op, fields provided, replacement
        filtered = filter_doc(
            {"op": "u", "o": {"a": 1, "b": 2, "c": 3, "d": 4}}, ["d", "e", "f"]
        )
        self.assertEqual(filtered, {"op": "u", "o": {"a": 1, "b": 2, "c": 3}})

    def test_filter_oplog_entry(self):
        # Test oplog entries: these are callables, since
        # filter_oplog_entry modifies the oplog entry in-place
        def insert_op():
            return {"op": "i", "o": {"_id": 0, "a": 1, "b": 2, "c": 3}}

        def update_op():
            return {
                "op": "u",
                "o": {"$set": {"a": 4, "b": 5}, "$unset": {"c": True}},
                "o2": {"_id": 1},
            }

        def filter_doc(document, fields):
            if fields and "_id" not in fields:
                fields.append("_id")
            return self.opman.filter_oplog_entry(document, include_fields=fields)

        # Case 0: insert op, no fields provided
        filtered = filter_doc(insert_op(), None)
        self.assertEqual(filtered, insert_op())

        # Case 1: insert op, fields provided
        filtered = filter_doc(insert_op(), ["a", "b"])
        self.assertEqual(filtered["o"], {"_id": 0, "a": 1, "b": 2})

        # Case 2: insert op, fields provided, doc becomes empty except for _id
        filtered = filter_doc(insert_op(), ["d", "e", "f"])
        self.assertEqual(filtered["o"], {"_id": 0})

        # Case 3: update op, no fields provided
        filtered = filter_doc(update_op(), None)
        self.assertEqual(filtered, update_op())

        # Case 4: update op, fields provided
        filtered = filter_doc(update_op(), ["a", "c"])
        self.assertNotIn("b", filtered["o"]["$set"])
        self.assertIn("a", filtered["o"]["$set"])
        self.assertEqual(filtered["o"]["$unset"], update_op()["o"]["$unset"])

        # Case 5: update op, fields provided, empty $set
        filtered = filter_doc(update_op(), ["c"])
        self.assertNotIn("$set", filtered["o"])
        self.assertEqual(filtered["o"]["$unset"], update_op()["o"]["$unset"])

        # Case 6: update op, fields provided, empty $unset
        filtered = filter_doc(update_op(), ["a", "b"])
        self.assertNotIn("$unset", filtered["o"])
        self.assertEqual(filtered["o"]["$set"], update_op()["o"]["$set"])

        # Case 7: update op, fields provided, entry is nullified
        filtered = filter_doc(update_op(), ["d", "e", "f"])
        self.assertEqual(filtered, None)

        # Case 8: update op, fields provided, replacement
        filtered = filter_doc(
            {"op": "u", "o": {"a": 1, "b": 2, "c": 3, "d": 4}}, ["a", "b", "c"]
        )
        self.assertEqual(filtered, {"op": "u", "o": {"a": 1, "b": 2, "c": 3}})

    def test_nested_fields(self):
        def check_nested(document, fields, filtered_document, op="i"):
            if "_id" not in fields:
                fields.append("_id")
            filtered_result = self.opman.filter_oplog_entry(
                {"op": op, "o": document}, include_fields=fields
            )
            if filtered_result is not None:
                filtered_result = filtered_result["o"]
            self.assertEqual(filtered_result, filtered_document)

        document = {"name": "Han Solo", "a": {"b": {}}}
        fields = ["name", "a.b.c"]
        filtered_document = {"name": "Han Solo"}
        check_nested(document, fields, filtered_document)

        document = {"a": {"b": {"c": 2, "e": 3}, "e": 5}, "b": 2, "c": {"g": 1}}
        fields = ["a.b.c", "a.e"]
        filtered_document = {"a": {"b": {"c": 2}, "e": 5}}
        check_nested(document, fields, filtered_document)

        document = {
            "a": {"b": {"c": 2, "e": 3}, "e": 5},
            "b": 2,
            "c": {"g": 1},
            "_id": 1,
        }
        fields = ["a.b.c", "a.e"]
        filtered_document = {"a": {"b": {"c": 2}, "e": 5}, "_id": 1}
        check_nested(document, fields, filtered_document)

        document = {"a": {"b": {"c": {"d": 1}}}, "-a": {"-b": {"-c": 2}}}
        fields = ["a.b", "-a"]
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)

        document = {"a": {"b": {"c": {"d": 1}}}, "-a": {"-b": {"-c": 2}}}
        fields = ["a", "-a.-b"]
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)
        document = {"a": {"b": {"c": {"d": 1}}}, "-a": {"-b": {"-c": 2}}, "_id": 1}

        fields = ["a.b", "-a"]
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)
        fields = ["a", "-a.-b"]
        check_nested(document, fields, filtered_document)

        document = {"test": 1}
        fields = ["doesnt_exist"]
        filtered_document = {}
        check_nested(document, fields, filtered_document)

        document = {"a": {"b": 1}, "b": {"a": 1}}
        fields = ["a.b", "b.a"]
        filtered_document = document.copy()
        check_nested(document, fields, filtered_document)

        document = {"a": {"b": {"a": {"b": 1}}}, "c": {"a": {"b": 1}}}
        fields = ["a.b"]
        filtered_document = {"a": {"b": {"a": {"b": 1}}}}
        check_nested(document, fields, filtered_document)

        document = {"name": "anna", "name_of_cat": "pushkin"}
        fields = ["name"]
        filtered_document = {"name": "anna"}
        check_nested(document, fields, filtered_document)

        update = {"$set": {"a.b": 1, "a.c": 3, "b": 2, "c": {"b": 3}}}
        fields = ["a", "c"]
        filtered_update = {"$set": {"a.b": 1, "a.c": 3, "c": {"b": 3}}}
        check_nested(update, fields, filtered_update, op="u")

        update = {"$set": {"a.b": {"c": 3, "d": 1}, "a.e": 1, "a.f": 2}}
        fields = ["a.b.c", "a.e"]
        filtered_update = {"$set": {"a.b": {"c": 3}, "a.e": 1}}
        check_nested(update, fields, filtered_update, op="u")

        update = {"$set": {"a.b.1": 1, "a.b.2": 2, "b": 3}}
        fields = ["a.b"]
        filtered_update = {"$set": {"a.b.1": 1, "a.b.2": 2}}
        check_nested(update, fields, filtered_update, op="u")

        update = {"$set": {"a.b": {"c": 3, "d": 1}, "a.e": 1}}
        fields = ["a.b.e"]
        filtered_update = None
        check_nested(update, fields, filtered_update, op="u")

    def test_nested_exclude_fields(self):
        def check_nested(document, exclude_fields, filtered_document, op="i"):
            if "_id" in exclude_fields:
                exclude_fields.remove("_id")
            filtered_result = self.opman.filter_oplog_entry(
                {"op": op, "o": document}, exclude_fields=exclude_fields
            )
            if filtered_result is not None:
                filtered_result = filtered_result["o"]
            self.assertEqual(filtered_result, filtered_document)

        document = {"a": {"b": {"c": {"d": 0, "e": 1}}}}
        exclude_fields = ["a.b.c.d"]
        filtered_document = {"a": {"b": {"c": {"e": 1}}}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": {"c": {"-a": 0, "d": {"e": {"f": 1}}}}}}
        exclude_fields = ["a.b.c.d.e.f"]
        filtered_document = {"a": {"b": {"c": {"-a": 0, "d": {"e": {}}}}}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": 1}
        exclude_fields = ["a"]
        filtered_document = {}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": {"c": 2, "e": 3}, "e": 5}, "b": 2, "c": {"g": 1}}
        exclude_fields = ["a.b.c", "a.e"]
        filtered_document = {"a": {"b": {"e": 3}}, "b": 2, "c": {"g": 1}}
        check_nested(document, exclude_fields, filtered_document)

        document = {
            "a": {"b": {"c": 2, "e": 3}, "e": 5},
            "b": 2,
            "c": {"g": 1},
            "_id": 1,
        }
        exclude_fields = ["a.b.c", "a.e", "_id"]
        filtered_document = {"a": {"b": {"e": 3}}, "b": 2, "c": {"g": 1}, "_id": 1}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": {"c": {"d": 1}}}, "-a": {"-b": {"-c": 2}}}
        exclude_fields = ["a.b", "-a"]
        filtered_document = {"a": {}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": {"c": {"d": 1}}}, "-a": {"-b": {"-c": 2}}}
        exclude_fields = ["a", "-a.-b"]
        filtered_document = {"-a": {}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": {"c": {"d": 1}}}, "-a": {"-b": {"-c": 2}}, "_id": 1}
        exclude_fields = ["a.b", "-a"]
        filtered_document = {"_id": 1, "a": {}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"test": 1}
        exclude_fields = ["doesnt_exist"]
        filtered_document = document.copy()
        check_nested(document, exclude_fields, filtered_document)

        document = {"test": 1}
        exclude_fields = ["test.doesnt_exist"]
        filtered_document = document.copy()
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": 1}, "b": {"a": 1}}
        exclude_fields = ["a.b", "b.a"]
        filtered_document = {"a": {}, "b": {}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"a": {"b": {"a": {"b": 1}}}, "c": {"a": {"b": 1}}}
        exclude_fields = ["a.b"]
        filtered_document = {"a": {}, "c": {"a": {"b": 1}}}
        check_nested(document, exclude_fields, filtered_document)

        document = {"name": "anna", "name_of_cat": "pushkin"}
        exclude_fields = ["name"]
        filtered_document = {"name_of_cat": "pushkin"}
        check_nested(document, exclude_fields, filtered_document)

        update = {"$set": {"a.b": 1, "a.c": 3, "b": 2, "c": {"b": 3}}}
        exclude_fields = ["a", "c"]
        filtered_update = {"$set": {"b": 2}}
        check_nested(update, exclude_fields, filtered_update, op="u")

        update = {"$set": {"a.b": {"c": 3, "d": 1}, "a.e": 1, "a.f": 2}}
        exclude_fields = ["a.b.c", "a.e"]
        filtered_update = {"$set": {"a.b": {"d": 1}, "a.f": 2}}
        check_nested(update, exclude_fields, filtered_update, op="u")

        update = {"$set": {"a.b": {"c": 3, "d": 1}, "a.e": 1}}
        exclude_fields = ["a.b.c", "a.b.d", "a.e"]
        filtered_update = {"$set": {"a.b": {}}}
        check_nested(update, exclude_fields, filtered_update, op="u")

        update = {"$set": {"a.b.1": 1, "a.b.2": 2, "b": 3}}
        exclude_fields = ["a.b"]
        filtered_update = {"$set": {"b": 3}}
        check_nested(update, exclude_fields, filtered_update, op="u")

        update = {"$set": {"a.b.c": 42, "d.e.f": 123, "g": 456}}
        exclude_fields = ["a.b", "d"]
        filtered_update = {"$set": {"g": 456}}
        check_nested(update, exclude_fields, filtered_update, op="u")

        update = {"$set": {"a.b": {"c": 3, "d": 1}, "a.e": 1}}
        exclude_fields = ["a.b", "a.e"]
        filtered_update = None
        check_nested(update, exclude_fields, filtered_update, op="u")


class TestFindFields(unittest.TestCase):
    def test_find_field(self):
        doc = {"a": {"b": {"c": 1}}}
        self.assertEqual(OplogThread._find_field("a", doc), [(["a"], doc["a"])])
        self.assertEqual(
            OplogThread._find_field("a.b", doc), [(["a", "b"], doc["a"]["b"])]
        )
        self.assertEqual(
            OplogThread._find_field("a.b.c", doc),
            [(["a", "b", "c"], doc["a"]["b"]["c"])],
        )
        self.assertEqual(OplogThread._find_field("x", doc), [])
        self.assertEqual(OplogThread._find_field("a.b.x", doc), [])

    def test_find_update_fields(self):
        doc = {"a": {"b": {"c": 1}}, "e.f": 1, "g.h": {"i": {"j": 1}}}
        self.assertEqual(OplogThread._find_update_fields("a", doc), [(["a"], doc["a"])])
        self.assertEqual(
            OplogThread._find_update_fields("a.b", doc), [(["a", "b"], doc["a"]["b"])]
        )
        self.assertEqual(
            OplogThread._find_update_fields("a.b.c", doc),
            [(["a", "b", "c"], doc["a"]["b"]["c"])],
        )
        self.assertEqual(OplogThread._find_update_fields("x", doc), [])
        self.assertEqual(OplogThread._find_update_fields("a.b.x", doc), [])
        self.assertEqual(
            OplogThread._find_update_fields("e.f", doc), [(["e.f"], doc["e.f"])]
        )
        self.assertEqual(
            OplogThread._find_update_fields("e", doc), [(["e.f"], doc["e.f"])]
        )
        self.assertEqual(
            OplogThread._find_update_fields("g.h.i.j", doc),
            [(["g.h", "i", "j"], doc["g.h"]["i"]["j"])],
        )

        # Test multiple matches
        doc = {"a.b": 1, "a.c": 2, "e.f.h": 3, "e.f.i": 4}
        matches = OplogThread._find_update_fields("a", doc)
        self.assertEqual(len(matches), 2)
        self.assertIn((["a.b"], doc["a.b"]), matches)
        self.assertIn((["a.c"], doc["a.c"]), matches)
        matches = OplogThread._find_update_fields("e.f", doc)
        self.assertEqual(len(matches), 2)
        self.assertIn((["e.f.h"], doc["e.f.h"]), matches)
        self.assertIn((["e.f.i"], doc["e.f.i"]), matches)

        # Test updates to array fields
        doc = {"a.b.1": 9, "a.b.3": 10, "a.b.4.c": 11}
        matches = OplogThread._find_update_fields("a.b", doc)
        self.assertEqual(len(matches), 3)
        self.assertIn((["a.b.1"], doc["a.b.1"]), matches)
        self.assertIn((["a.b.3"], doc["a.b.3"]), matches)
        self.assertIn((["a.b.4.c"], doc["a.b.4.c"]), matches)


if __name__ == "__main__":
    unittest.main()
