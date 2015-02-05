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

sys.path[0:0] = [""]

from mongo_connector.util import (bson_ts_to_long,
                                  long_to_bson_ts,
                                  retry_until_ok)
from tests import unittest


def err_func():
    """Helper function for retry_until_ok test
    """

    err_func.counter += 1
    if err_func.counter == 3:
        return True
    else:
        raise TypeError

err_func.counter = 0


class TestUtil(unittest.TestCase):
    """ Tests the utils
    """

    def test_bson_ts_to_long(self):
        """Test bson_ts_to_long and long_to_bson_ts
        """

        tstamp = timestamp.Timestamp(0x12345678, 0x90abcdef)

        self.assertEqual(0x1234567890abcdef,
                         bson_ts_to_long(tstamp))
        self.assertEqual(long_to_bson_ts(0x1234567890abcdef),
                         tstamp)

    def test_retry_until_ok(self):
        """Test retry_until_ok
        """

        self.assertTrue(retry_until_ok(err_func))
        self.assertEqual(err_func.counter, 3)


if __name__ == '__main__':

    unittest.main()
