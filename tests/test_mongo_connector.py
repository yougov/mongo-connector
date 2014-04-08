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
from mongo_connector.connector import Connector
from tests.setup_cluster import start_cluster, kill_all, PORTS_ONE
from bson.timestamp import Timestamp
from mongo_connector import errors
from mongo_connector.doc_managers import (
    doc_manager_simulator
)
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
        assert(start_cluster())

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def test_connector(self):
        """Test whether the connector initiates properly
        """
        conn = Connector(
            address="localhost:%s" % PORTS_ONE["PRIMARY"],
            oplog_checkpoint='config.txt',
            target_url=None,
            ns_set=['test.test'],
            u_key='_id',
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
            address="localhost:%s" % PORTS_ONE["PRIMARY"],
            oplog_checkpoint="temp_config.txt",
            target_url=None,
            ns_set=['test.test'],
            u_key='_id',
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

        os.unlink("temp_config.txt")
        config_file.close()

    def test_read_oplog_progress(self):
        """Test read_oplog_progress
        """

        conn = Connector(
            address="localhost:%s" % PORTS_ONE["PRIMARY"],
            oplog_checkpoint=None,
            target_url=None,
            ns_set=['test.test'],
            u_key='_id',
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

    def test_many_targets(self):
        """Test that DocManagers are created and assigned to target URLs
        correctly when instantiating a Connector object with multiple target
        URLs
        """

        # no doc manager or target URLs
        connector_kwargs = {
            "address": "localhost:%s" % PORTS_ONE["PRIMARY"],
            "oplog_checkpoint": None,
            "ns_set": None,
            "u_key": None,
            "auth_key": None
        }
        c = Connector(target_url=None, **connector_kwargs)
        self.assertEqual(len(c.doc_managers), 1)
        self.assertIsInstance(c.doc_managers[0],
                              doc_manager_simulator.DocManager)

        # N.B. This assumes we're in mongo-connector/tests
        def get_docman(name):
            return os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                os.pardir,
                "mongo_connector",
                "doc_managers",
                "%s.py" % name
            )

        # only target URL provided
        with self.assertRaises(errors.ConnectorError):
            Connector(target_url="localhost:9200", **connector_kwargs)

        # one doc manager taking a target URL, no URL provided
        with self.assertRaises(TypeError):
            c = Connector(doc_manager=get_docman("mongo_doc_manager"),
                          **connector_kwargs)

        # 1:1 target URLs and doc managers
        c = Connector(
            doc_manager=[
                get_docman("elastic_doc_manager"),
                get_docman("doc_manager_simulator"),
                get_docman("elastic_doc_manager")
            ],
            target_url=[
                "localhost:%s" % PORTS_ONE['PRIMARY'],
                "foobar",
                "bazbaz"
            ],
            **connector_kwargs
        )
        self.assertEqual(len(c.doc_managers), 3)
        # Connector uses doc manager filename as module name
        self.assertEqual(c.doc_managers[0].__module__,
                         "elastic_doc_manager")
        self.assertEqual(c.doc_managers[1].__module__,
                         "doc_manager_simulator")
        self.assertEqual(c.doc_managers[2].__module__,
                         "elastic_doc_manager")

        # more target URLs than doc managers
        c = Connector(
            doc_manager=[
                get_docman("doc_manager_simulator")
            ],
            target_url=[
                "localhost:%s" % PORTS_ONE["PRIMARY"],
                "foobar",
                "bazbaz"
            ],
            **connector_kwargs
        )
        self.assertEqual(len(c.doc_managers), 3)
        self.assertEqual(c.doc_managers[0].__module__,
                         "doc_manager_simulator")
        self.assertEqual(c.doc_managers[1].__module__,
                         "doc_manager_simulator")
        self.assertEqual(c.doc_managers[2].__module__,
                         "doc_manager_simulator")
        self.assertEqual(c.doc_managers[0].url,
                         "localhost:%s" % PORTS_ONE["PRIMARY"])
        self.assertEqual(c.doc_managers[1].url, "foobar")
        self.assertEqual(c.doc_managers[2].url, "bazbaz")

        # more doc managers than target URLs
        c = Connector(
            doc_manager=[
                get_docman("elastic_doc_manager"),
                get_docman("doc_manager_simulator"),
                get_docman("doc_manager_simulator")
            ],
            target_url=[
                "localhost:%s" % PORTS_ONE["PRIMARY"]
            ],
            **connector_kwargs
        )
        self.assertEqual(len(c.doc_managers), 3)
        self.assertEqual(c.doc_managers[0].__module__,
                         "elastic_doc_manager")
        self.assertEqual(c.doc_managers[1].__module__,
                         "doc_manager_simulator")
        self.assertEqual(c.doc_managers[2].__module__,
                         "doc_manager_simulator")
        # extra doc managers should have None as target URL
        self.assertEqual(c.doc_managers[1].url, None)
        self.assertEqual(c.doc_managers[2].url, None)

if __name__ == '__main__':
    unittest.main()
