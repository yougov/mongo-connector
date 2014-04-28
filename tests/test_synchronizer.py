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

"""Test synchronizer using DocManagerSimulator
"""
import os
import sys

sys.path[0:0] = [""]

from pymongo import MongoClient

import time
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
from tests import mongo_host
from tests.setup_cluster import (start_replica_set,
                                 kill_all)
from tests.util import assert_soon
from mongo_connector.connector import Connector


class TestSynchronizer(unittest.TestCase):
    """ Tests the synchronizers
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the cluster
        """
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()

        _, _, cls.primary_p = start_replica_set('test-synchronizer')
        cls.conn = MongoClient('%s:%d' % (mongo_host, cls.primary_p),
                               replicaSet='test-synchronizer')
        cls.connector = Connector(
            address='%s:%d' % (mongo_host, cls.primary_p),
            oplog_checkpoint='config.txt',
            target_url=None,
            ns_set=['test.test'],
            u_key='_id',
            auth_key=None
        )
        cls.synchronizer = cls.connector.doc_managers[0]
        cls.connector.start()
        assert_soon(lambda: len(cls.connector.shard_set) != 0)

    @classmethod
    def tearDownClass(cls):
        """ Tears down connector
        """
        cls.connector.join()
        kill_all()

    def setUp(self):
        """ Clears the db
        """
        self.conn['test']['test'].remove()
        assert_soon(lambda: len(self.synchronizer._search()) == 0)

    def test_insert(self):
        """Tests insert
        """
        self.conn['test']['test'].insert({'name': 'paulie'})
        while (len(self.synchronizer._search()) == 0):
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], result_set_2['_id'])
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """
        self.conn['test']['test'].insert({'name': 'paulie'})
        while (len(self.synchronizer._search()) != 1):
            time.sleep(1)
        self.conn['test']['test'].remove({'name': 'paulie'})

        while (len(self.synchronizer._search()) == 1):
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 0)


if __name__ == '__main__':
    unittest.main()
