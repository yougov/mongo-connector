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

"""A set of utilities used throughout the mongo-connector
"""

import time
import logging

from bson.timestamp import Timestamp
try:
    from urllib2 import urlopen, URLError
except ImportError:
    from urllib.request import urlopen, URLError


def bson_ts_to_long(timestamp):
    """Convert BSON timestamp into integer.

    Conversion rule is based from the specs
    (http://bsonspec.org/#/specification).
    """
    return ((timestamp.time << 32) + timestamp.inc)


def long_to_bson_ts(val):
    """Convert integer into BSON timestamp.
    """
    seconds = val >> 32
    increment = val & 0xffffffff

    return Timestamp(seconds, increment)


def retry_until_ok(func, *args, **kwargs):
    """Retry code block until it succeeds.

    If it does not succeed in 60 attempts, the function re-raises any
    error the function raised on its last attempt.

    """

    count = 0
    while True:
        try:
            return func(*args, **kwargs)
        except:
            count += 1
            if count > 60:
                logging.error('Call to %s failed too many times in '
                              'retry_until_ok', func)
                raise
            time.sleep(1)
