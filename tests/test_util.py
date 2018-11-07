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

"""Tests methods in util.py
"""
import sys

from bson import timestamp
from pymongo import errors

sys.path[0:0] = [""]  # noqa

from mongo_connector.util import bson_ts_to_long, long_to_bson_ts, retry_until_ok
from tests import unittest


def err_func(first_error):
    """Helper function for retry_until_ok test
    """
    err_func.counter += 1
    if err_func.counter == 3:
        return True
    elif err_func.counter == 2:
        raise TypeError
    else:
        raise first_error


err_func.counter = 0


class TestUtil(unittest.TestCase):
    """ Tests the utils
    """

    def setUp(self):
        err_func.counter = 0

    def test_bson_ts_to_long(self):
        """Test bson_ts_to_long and long_to_bson_ts
        """

        tstamp = timestamp.Timestamp(0x12345678, 0x90ABCDEF)

        self.assertEqual(0x1234567890ABCDEF, bson_ts_to_long(tstamp))
        self.assertEqual(long_to_bson_ts(0x1234567890ABCDEF), tstamp)

    def test_retry_until_ok(self):
        """Test retry_until_ok
        """
        self.assertTrue(retry_until_ok(err_func, errors.ConnectionFailure()))
        self.assertEqual(err_func.counter, 3)

    def test_retry_until_ok_operation_failure(self):
        """Test retry_until_ok retries on PyMongo OperationFailure.
        """
        self.assertTrue(retry_until_ok(err_func, errors.OperationFailure("")))
        self.assertEqual(err_func.counter, 3)

    def test_retry_until_ok_authorization(self):
        """Test retry_until_ok does not mask authorization failures.
        """
        with self.assertRaises(errors.OperationFailure):
            retry_until_ok(err_func, errors.OperationFailure("", 13, None))
        self.assertEqual(err_func.counter, 1)

    def test_retry_until_ok_authorization_mongodb_24(self):
        """Test retry_until_ok does not mask authorization failures in
        MongoDB 2.4.
        """
        with self.assertRaises(errors.OperationFailure):
            retry_until_ok(
                err_func,
                errors.OperationFailure("", details={"errmsg": "unauthorized"}),
            )
        self.assertEqual(err_func.counter, 1)

    def test_retry_until_ok_runtime_error(self):
        """Test retry_until_ok does not mask RuntimeErrors.
        """
        with self.assertRaises(RuntimeError):
            retry_until_ok(err_func, RuntimeError)
        self.assertEqual(err_func.counter, 1)


if __name__ == "__main__":

    unittest.main()
