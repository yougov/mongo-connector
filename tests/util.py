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

"""Utilities for mongo-connector tests. There are no actual tests in here.
"""

import time

def wait_for(condition, max_tries=60):
    """Wait for a condition to be true up to a maximum number of tries
    """
    while not condition() and max_tries > 1:
        time.sleep(1)
        max_tries -= 1
    return condition()
