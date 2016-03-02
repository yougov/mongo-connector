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

import logging
import sys

logging.basicConfig(stream=sys.stdout)

if sys.version_info[0] == 3:
    unicode = str

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
    from unittest2.case import SkipTest
else:
    import unittest
    from unittest.case import SkipTest

