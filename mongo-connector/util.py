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

"""A set of utilities used throughout the mongo-connector
"""

import sys
import time
import logging

from bson.timestamp import Timestamp
from pymongo import Connection
try:
    from urllib2 import urlopen
except:
    from urllib.request import urlopen


def verify_url(url):
    """Verifies the validity of a given url.
    """
    try:
        urlopen(url)
        return True
    except:
        return False


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


def retry_until_ok(func, args=None):
    """Retry code block until it succeeds.

    If it does not succeed in 60 attempts, the
    function simply exits.
    """

    result = True
    count = 0
    while True:
        try:
            if args is None:
                result = func()
                break
            else:
                result = func(args)
                break
        except:
            count += 1
            if count > 60:
                string = 'Call to %s failed too many times' % func
                string += ' in retry_until_ok'
                logging.error(string)
                sys.exit(1)
            time.sleep(1)

    return result
