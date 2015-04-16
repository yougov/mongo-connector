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
elastic_host = unicode(os.environ.get("ES_HOST", 'localhost'))
elastic_port = unicode(os.environ.get("ES_PORT", 9200))
elastic_pair = '%s:%s' % (elastic_host, elastic_port)
solr_url = unicode(os.environ.get('SOLR_URL', 'http://localhost:8983/solr'))
db_user = unicode(os.environ.get("DB_USER", ""))
db_password = unicode(os.environ.get("DB_PASSWORD", ""))
# Extra keyword options to provide to Connector.
connector_opts = {}
if db_user:
    connector_opts = {'auth_username': db_user, 'auth_key': db_password}

# Document count for stress tests
STRESS_COUNT = 100

# Test namespace, timestamp arguments
TESTARGS = ('test.test', 1)
