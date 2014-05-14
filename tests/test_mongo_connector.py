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

"""Tests methods for mongo_connector
"""

import os
import sys

sys.path[0:0] = [""]


if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
import time
import json

from mongo_connector.connector import Connector, create_doc_managers
from tests import mongo_host
from tests.setup_cluster import start_replica_set, kill_replica_set
from bson.timestamp import Timestamp
from mongo_connector.util import long_to_bson_ts


class TestMongoConnector(unittest.TestCase):
    """ Test Class for the Mongo Connector
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
        _, _, cls.primary_p = start_replica_set('test-mongo-connector')

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_replica_set('test-mongo-connector')

    def test_connector(self):
        """Test whether the connector initiates properly
        """
        conn = Connector(
            address='%s:%d' % (mongo_host, self.primary_p),
            oplog_checkpoint='config.txt',
            ns_set=['test.test'],
            auth_key=None
        )
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
        try:
            os.unlink("temp_config.txt")
        except OSError:
            pass
        open("temp_config.txt", "w").close()
        conn = Connector(
            address='%s:%d' % (mongo_host, self.primary_p),
            oplog_checkpoint="temp_config.txt",
            ns_set=['test.test'],
            auth_key=None
        )

        #test that None is returned if there is no config file specified.
        self.assertEqual(conn.write_oplog_progress(), None)

        conn.oplog_progress.get_dict()[1] = Timestamp(12, 34)
        #pretend to insert a thread/timestamp pair
        conn.write_oplog_progress()

        data = json.load(open("temp_config.txt", 'r'))
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(12, 34))

        #ensure the temp file was deleted
        self.assertFalse(os.path.exists("temp_config.txt" + '~'))

        #ensure that updates work properly
        conn.oplog_progress.get_dict()[1] = Timestamp(44, 22)
        conn.write_oplog_progress()

        config_file = open("temp_config.txt", 'r')
        data = json.load(config_file)
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(44, 22))

        config_file.close()
        os.unlink("temp_config.txt")

    def test_read_oplog_progress(self):
        """Test read_oplog_progress
        """

        conn = Connector(
            address='%s:%d' % (mongo_host, self.primary_p),
            oplog_checkpoint=None,
            ns_set=['test.test'],
            auth_key=None
        )

        #testing with no file
        self.assertEqual(conn.read_oplog_progress(), None)

        try:
            os.unlink("temp_config.txt")
        except OSError:
            pass
        open("temp_config.txt", "w").close()

        conn.oplog_checkpoint = "temp_config.txt"

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

        os.unlink("temp_config.txt")

    def test_create_doc_managers(self):
        """Test that DocManagers are created correctly for given CLI options."""
        # only target URL provided
        with self.assertRaises(SystemExit):
            create_doc_managers(urls="abcxyz.com")

        # one doc manager taking a target URL, no URL provided
        with self.assertRaises(SystemExit):
            create_doc_managers(names="solr_doc_manager")

        # 1:1 target URLs and doc managers
        names = ["elastic_doc_manager",
                 "doc_manager_simulator",
                 "elastic_doc_manager"]
        urls = ['%s:%d' % (mongo_host, self.primary_p),
                "foobar",
                "bazbaz"]
        doc_managers = create_doc_managers(
            names=",".join(names),
            urls=",".join(urls)
        )
        self.assertEqual(len(doc_managers), 3)
        # Connector uses doc manager filename as module name
        for index, name in enumerate(names):
            self.assertEqual(doc_managers[index].__module__,
                             "mongo_connector.doc_managers.%s" % name)

        # more target URLs than doc managers
        doc_managers = create_doc_managers(
            names="doc_manager_simulator",
            urls=",".join(urls))
        self.assertEqual(len(doc_managers), 3)
        for dm in doc_managers:
            self.assertEqual(
                dm.__module__,
                "mongo_connector.doc_managers.doc_manager_simulator")
        for index, url in enumerate(urls):
            self.assertEqual(doc_managers[index].url, url)

        # more doc managers than target URLs
        names = ["elastic_doc_manager",
                 "doc_manager_simulator",
                 "doc_manager_simulator"]
        doc_managers = create_doc_managers(
            names=",".join(names),
            urls='%s:%d' % (mongo_host, self.primary_p))
        for index, name in enumerate(names):
            self.assertEqual(
                doc_managers[index].__module__,
                "mongo_connector.doc_managers.%s" % name)
        self.assertEqual(doc_managers[1].url, None)
        self.assertEqual(doc_managers[2].url, None)


if __name__ == '__main__':
    unittest.main()
