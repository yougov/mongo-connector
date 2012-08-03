# Copyright 2012 10gen, Inc.
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

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Tests methods in util.py
"""

import os
import sys
import inspect
file = inspect.getfile(inspect.currentframe())
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))
cmd_folder = cmd_folder.rsplit("/", 1)[0]
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

import unittest
from bson import timestamp
from util import (verify_url,
                  bson_ts_to_long,
                  long_to_bson_ts,
                  retry_until_ok)


def err_func():
    """Helper function for retry_until_ok test
    """

    err_func.counter += 1
    if err_func.counter == 3:
        return True
    else:
        raise TypeError

err_func.counter = 0


class UtilTester(unittest.TestCase):

    def runTest(self):
        super(UtilTester, self).__init__()

    def test_verify_url(self):
        """Test verify_url with good and bad urls
        """

        bad_url = "weofkej"
        good_url = "http://www.google.com"
        no_http_url = "www.google.com"
        good_host_bad_path = "http://www.google.com/-##4@3weo$%*"

        self.assertTrue(verify_url(good_url))

        self.assertFalse(verify_url(no_http_url))
        self.assertFalse(verify_url(bad_url))
        self.assertFalse(verify_url(good_host_bad_path))
        print("PASSED TEST VERIFY URL")

    def test_bson_ts_to_long(self):
        """Test bson_ts_to_long and long_to_bson_ts
        """

        ts = timestamp.Timestamp(0x12345678, 0x90abcdef)

        self.assertEqual(0x1234567890abcdef, bson_ts_to_long(ts))
        self.assertEqual(long_to_bson_ts(0x1234567890abcdef), ts)
        print("PASSED BSON TS TO LONG")

    def test_retry_until_ok(self):
        """Test retry_until_ok
        """

        self.assertTrue(retry_until_ok(err_func))
        self.assertEqual(err_func.counter, 3)
        print("PASSED RETRY UNTIL OK")


if __name__ == '__main__':

    unittest.main()
