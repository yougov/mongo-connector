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

"""Tests methods for mongo_connector
"""

import os
import sys
import inspect
import socket

sys.path[0:0] = [""]

import unittest
import time
import json
from mongo_connector.mongo_connector import Connector
from tests.setup_cluster import start_cluster, kill_all
from bson.timestamp import Timestamp
from mongo_connector.util import long_to_bson_ts

HOSTNAME = os.environ.get('HOSTNAME', socket.gethostname())
MAIN_ADDR = os.environ.get('MAIN_ADDR', "27217")
MAIN_ADDRESS = "%s:%s" % (HOSTNAME, MAIN_ADDR)
CONFIG = os.environ.get('CONFIG', "config.txt")
TEMP_CONFIG = os.environ.get('TEMP_CONFIG', "temp_config.txt")


class MongoInternalTester(unittest.TestCase):
    """ Test Class for the Mongo Connector
    """

    def runTest(self):
        """ Runs the tests
        """

        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):    
        """ Initializes the cluster
        """

        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        use_mongos = True
        if MAIN_ADDRESS.split(":")[1] != "27217":
            use_mongos = False
        cls.flag = start_cluster(use_mongos=use_mongos)

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def test_connector(self):
        """Test whether the connector initiates properly
        """
        if not self.flag:
            self.fail("Shards cannot be added to mongos")

        conn = Connector(MAIN_ADDRESS, CONFIG, None, ['test.test'],
                      '_id', None, None)
        conn.start()

        while len(conn.shard_set) != 1:
            time.sleep(2)
        conn.join()

        self.assertFalse(conn.can_run)
        time.sleep(5)
        for thread in conn.shard_set.values():
            self.assertFalse(thread.running)

    def test_write_oplog_progress(self):
        """Test write_oplog_progress under several circumstances
        """
        os.system('touch %s' % (TEMP_CONFIG))
        config_file_path = TEMP_CONFIG
        conn = Connector(MAIN_ADDRESS, config_file_path, None, ['test.test'],
                      '_id', None, None)

        #test that None is returned if there is no config file specified.
        self.assertEqual(conn.write_oplog_progress(), None)

        conn.oplog_progress.get_dict()[1] = Timestamp(12, 34)
        #pretend to insert a thread/timestamp pair
        conn.write_oplog_progress()

        data = json.load(open(config_file_path, 'r'))
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(12, 34))

        #ensure the temp file was deleted
        self.assertFalse(os.path.exists(config_file_path + '~'))

        #ensure that updates work properly
        conn.oplog_progress.get_dict()[1] = Timestamp(44, 22)
        conn.write_oplog_progress()

        config_file = open(config_file_path, 'r')
        data = json.load(config_file)
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(44, 22))

        os.system('rm ' + config_file_path)
        config_file.close()

    def test_read_oplog_progress(self):
        """Test read_oplog_progress
        """

        conn = Connector(MAIN_ADDRESS, None, None, ['test.test'], '_id',
                      None, None)

        #testing with no file
        self.assertEqual(conn.read_oplog_progress(), None)

        os.system('touch %s' % (TEMP_CONFIG))
        conn.oplog_checkpoint = TEMP_CONFIG

        #testing with empty file
        self.assertEqual(conn.read_oplog_progress(), None)

        oplog_dict = conn.oplog_progress.get_dict()

        #add a value to the file, delete the dict, and then read in the value
        oplog_dict['oplog1'] = Timestamp(12, 34)
        conn.write_oplog_progress()
        del oplog_dict['oplog1']

        self.assertEqual(len(oplog_dict), 0)

        conn.read_oplog_progress()

        self.assertTrue('oplog1' in oplog_dict.keys())
        self.assertTrue(oplog_dict['oplog1'], Timestamp(12, 34))

        oplog_dict['oplog1'] = Timestamp(55, 11)

        #see if oplog progress dict is properly updated
        conn.read_oplog_progress()
        self.assertTrue(oplog_dict['oplog1'], Timestamp(55, 11))

        os.system('rm ' + TEMP_CONFIG)

if __name__ == '__main__':
    unittest.main()
