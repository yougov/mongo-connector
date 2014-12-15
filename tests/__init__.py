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
import os
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

# Configurable hosts and ports used in the tests
mongo_host = unicode(os.environ.get("MONGO_HOST", 'localhost'))
mongo_start_port = int(os.environ.get("MONGO_PORT", 27017))
elastic_host = unicode(os.environ.get("ES_HOST", 'localhost'))
elastic_port = unicode(os.environ.get("ES_PORT", 9200))
elastic_pair = '%s:%s' % (elastic_host, elastic_port)
solr_host = unicode(os.environ.get("SOLR_HOST", 'localhost'))
solr_port = unicode(os.environ.get("SOLR_PORT", 8983))
solr_pair = '%s:%s' % (solr_host, solr_port)

# Document count for stress tests
STRESS_COUNT = 100

# Test namespace, timestamp arguments
TESTARGS = ('test.test', 1)
