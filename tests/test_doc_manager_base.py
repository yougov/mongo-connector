# Copyright 2017 MongoDB, Inc.
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

import sys

sys.path[0:0] = [""]  # noqa

from mongo_connector.connector import (
    get_mininum_mongodb_version,
    update_mininum_mongodb_version,
)
from mongo_connector.doc_managers.doc_manager_base import (
    DocManagerBase,
    UpdateDoesNotApply,
)
from mongo_connector.test_utils import TESTARGS
from mongo_connector.version import Version

from tests import unittest


UPDATE_SUCCESS_TEST_CASES = [
    {
        "description": "Update whole document.",
        "doc": {"a": 1},
        "update_spec": {"a": 2, "b": 3},
        "result": {"a": 2, "b": 3},
    },
    {
        "description": "Update by un-setting an existing field.",
        "doc": {"a": 1, "b": 2},
        "update_spec": {"$unset": {"a": True}},
        "result": {"b": 2},
    },
    {
        "description": "Update by un-setting an existing nested field.",
        "doc": {"a": {"b": 2, "c": 3}},
        "update_spec": {"$unset": {"a.b": True}},
        "result": {"a": {"c": 3}},
    },
    {
        "description": "Update by un-setting an array element.",
        "doc": {"a": 1, "b": [0, 1, 2, 3]},
        "update_spec": {"$unset": {"b.1": True}},
        "result": {"a": 1, "b": [0, None, 2, 3]},
    },
    {
        "description": "Update by adding a field.",
        "doc": {"a": 1},
        "update_spec": {"$set": {"b": [{"c": 1}, {"d": 2}]}},
        "result": {"a": 1, "b": [{"c": 1}, {"d": 2}]},
    },
    {
        "description": "Update by adding a nested field.",
        "doc": {"a": 1},
        "update_spec": {"$set": {"b.c.d": 2}},
        "result": {"a": 1, "b": {"c": {"d": 2}}},
    },
    {
        "description": "Update by adding and removing a fields.",
        "doc": {"a": 1, "c": 3},
        "update_spec": {"$unset": {"a": True, "c": True}, "$set": {"b": 2, "d": 4}},
        "result": {"b": 2, "d": 4},
    },
    {
        "description": "Update by setting an element far beyond the end of" "an array.",
        "doc": {"a": 1, "b": [{"c": 1}]},
        "update_spec": {"$set": {"b.4": {"c": 2}}},
        "result": {"a": 1, "b": [{"c": 1}, None, None, None, {"c": 2}]},
    },
    {
        "description": "Update by setting an element right beyond the end "
        "of an array.",
        "doc": {"a": 1, "b": [{"c": 1}]},
        "update_spec": {"$set": {"b.1": {"c": 2}}},
        "result": {"a": 1, "b": [{"c": 1}, {"c": 2}]},
    },
    {
        "description": "Update by setting an attribute of a sub-document far "
        "beyond the end of an array.",
        "doc": {"a": 1, "b": [{"c": 1}]},
        "update_spec": {"$set": {"b.4.c": 2}},
        "result": {"a": 1, "b": [{"c": 1}, None, None, None, {"c": 2}]},
    },
    {
        "description": "Update by setting an attribute of a sub-document "
        "right beyond the end of an array.",
        "doc": {"a": 1, "b": [{"c": 1}]},
        "update_spec": {"$set": {"b.1.c": 2}},
        "result": {"a": 1, "b": [{"c": 1}, {"c": 2}]},
    },
    {
        "description": "Update by changing a field within an array element.",
        "doc": {"a": 1, "b": [{"c": 1}]},
        "update_spec": {"$set": {"b.0.c": 2}},
        "result": {"a": 1, "b": [{"c": 2}]},
    },
    {
        "description": "Update by adding a field within an array element.",
        "doc": {"a": 1, "b": [{"c": 1}, {"d": 2}]},
        "update_spec": {"$set": {"b.1.c": 3}},
        "result": {"a": 1, "b": [{"c": 1}, {"c": 3, "d": 2}]},
    },
    {
        "description": "Update by replacing an array element.",
        "doc": {"a": 1, "b": [0, 1, 2, 3]},
        "update_spec": {"$set": {"b.2": {"new": 2}}},
        "result": {"a": 1, "b": [0, 1, {"new": 2}, 3]},
    },
    {
        "description": "Update by replacing an array with a document with "
        "int field.",
        "doc": {"a": 1, "b": [{"c": 1}, {"d": 2}]},
        "update_spec": {"$set": {"b": {"0": {"e": 100}}}},
        "result": {"a": 1, "b": {"0": {"e": 100}}},
    },
]

UNSET_FAILURE_TEST_CASES = [
    {
        "description": "Update by un-setting a non-existent field.",
        "doc": {"a": 1, "b": 2},
        "update_spec": {"$unset": {"not-present": True}},
        "result": {"a": 1, "b": 2},
    },
    {
        "description": "Update by un-setting a non-existent nested field.",
        "doc": {"a": 1, "b": {"c": {"d": 1}}},
        "update_spec": {"$unset": {"b.not-present.foo": True}},
        "result": {"a": 1, "b": {"c": {"d": 1}}},
    },
    {
        "description": "Update by un-setting invalid array index.",
        "doc": {"a": 1, "b": [0, 1, 2, 3]},
        "update_spec": {"$unset": {"b.not-an-index": True}},
        "result": {"a": 1, "b": [0, 1, 2, 3]},
    },
    {
        "description": "Update by un-setting invalid nested array index.",
        "doc": {"a": 1, "b": [0, 1, 2, 3]},
        "update_spec": {"$unset": {"b.not-an-index.not-present": True}},
        "result": {"a": 1, "b": [0, 1, 2, 3]},
    },
    {
        "description": "Update by un-setting a non-existent array element.",
        "doc": {"a": 1, "b": [0, 1, 2]},
        "update_spec": {"$unset": {"b.4": True}},
        "result": {"a": 1, "b": [0, 1, 2]},
    },
    {
        "description": "Update by un-setting a non-existent field in an array"
        "element.",
        "doc": {"a": 1, "b": [0, {"c": 1}, 2]},
        "update_spec": {"$unset": {"b.1.not-present": True}},
        "result": {"a": 1, "b": [0, {"c": 1}, 2]},
    },
    {
        "description": "Update by adding and removing a non-existent field.",
        "doc": {"a": 1},
        "update_spec": {"$unset": {"a": True, "not-present": True}, "$set": {"b": 2}},
        "result": {"b": 2},
    },
]

UPDATE_FAILURE_TEST_CASES = [
    {
        "description": "Using array notation on non-array field.",
        "doc": {"a": 1},
        "update_spec": {"$set": {"a.0": 2}},
    },
    {
        "description": "Using array notation on non-array field.",
        "doc": {"a": 1},
        "update_spec": {"$set": {"a.0.1": 2}},
    },
    {
        "description": "Using nested field notation on non-object.",
        "doc": {"a": 1},
        "update_spec": {"$set": {"a.b": 2}},
    },
    {
        "description": "Using deeply nested field notation on non-object.",
        "doc": {"a": 1},
        "update_spec": {"$set": {"a.b.c.b": 2}},
    },
    {
        "description": "Setting a field on an array field.",
        "doc": {"a": [{"c": 1}, {"c": 2}]},
        "update_spec": {"$set": {"a.c": 2}},
    },
    {
        "description": "Setting a field on a null array element.",
        "doc": {"a": [None, None]},
        "update_spec": {"$set": {"a.0.c": 2}},
    },
]


class TestDocManagerBase(unittest.TestCase):
    """Unit tests for DocManagerBase"""

    def setUp(self):
        self.base = DocManagerBase()

    def assertUpdateTestSucceeds(self, test):
        self.assertEqual(
            self.base.apply_update(test["doc"], test["update_spec"]),
            test["result"],
            msg=test["description"],
        )

    def assertUpdateTestFails(self, test):
        try:
            doc = self.base.apply_update(test["doc"], test["update_spec"])
            self.fail(
                "UpdateDoesNotApply on MongoDB verison %s not raised for "
                "test: %s, applied %r to %r and got %r"
                % (
                    get_mininum_mongodb_version(),
                    test["description"],
                    test["update_spec"],
                    test["doc"],
                    doc,
                )
            )
        except UpdateDoesNotApply:
            pass

    def test_apply_update(self):
        for test in UPDATE_SUCCESS_TEST_CASES:
            self.assertUpdateTestSucceeds(test)

    def test_apply_update_fails(self):
        for test in UPDATE_FAILURE_TEST_CASES:
            self.assertUpdateTestFails(test)

    def test_apply_update_unset_failures(self):
        # Reset the minimum MongoDB version at the start and end.
        update_mininum_mongodb_version(None)
        for mock_mongodb_version in [(3, 4), (3, 2), (3, 0), (2, 6), (2, 4), None]:
            if mock_mongodb_version is None:
                update_mininum_mongodb_version(None)
            else:
                update_mininum_mongodb_version(Version(*mock_mongodb_version))
            for test in UNSET_FAILURE_TEST_CASES:
                if mock_mongodb_version == (2, 4):
                    self.assertUpdateTestSucceeds(test)
                else:
                    self.assertUpdateTestFails(test)

    def test_bulk_upsert(self):
        with self.assertRaises(NotImplementedError):
            self.base.bulk_upsert([{}], *TESTARGS)

    def test_update(self):
        with self.assertRaises(NotImplementedError):
            self.base.update({}, {}, *TESTARGS)

    def test_upsert(self):
        with self.assertRaises(NotImplementedError):
            self.base.upsert({}, *TESTARGS)

    def test_remove(self):
        with self.assertRaises(NotImplementedError):
            self.base.remove(1, *TESTARGS)

    def test_insert_file(self):
        with self.assertRaises(NotImplementedError):
            self.base.insert_file(None, *TESTARGS)

    def test_handle_command(self):
        with self.assertRaises(NotImplementedError):
            self.base.handle_command({}, *TESTARGS)

    def test_search(self):
        with self.assertRaises(NotImplementedError):
            self.base.search(0, 1)

    def test_commit(self):
        with self.assertRaises(NotImplementedError):
            self.base.commit()

    def test_get_last_doc(self):
        with self.assertRaises(NotImplementedError):
            self.base.get_last_doc()

    def test_stop(self):
        with self.assertRaises(NotImplementedError):
            self.base.stop()


if __name__ == "__main__":
    unittest.main()
