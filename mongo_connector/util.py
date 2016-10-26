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

import logging
import sys
import time

from bson.timestamp import Timestamp

from mongo_connector.compat import reraise

LOG = logging.getLogger(__name__)


def exception_wrapper(mapping):
    def decorator(f):
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                exc_type, exc_value, exc_tb = sys.exc_info()
                new_type = None
                for src_type in mapping:
                    if issubclass(exc_type, src_type):
                        new_type = mapping[src_type]
                        break

                if new_type is None:
                    raise
                reraise(new_type, exc_value, exc_tb)
        return wrapped
    return decorator


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

    If it does not succeed in 120 attempts, the function re-raises any
    error the function raised on its last attempt.

    """

    count = 0
    while True:
        try:
            return func(*args, **kwargs)
        except RuntimeError:
            # Avoid masking RuntimeErrors
            raise
        except Exception:
            count += 1
            if count > 120:
                LOG.exception('Call to %s failed too many times in '
                              'retry_until_ok', func)
                raise
            time.sleep(1)


def log_fatal_exceptions(func):
    def wrapped(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:
            LOG.exception("Fatal Exception")
            raise
    return wrapped
