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
from tests.setup_cluster import (kill_mongo_proc,
                                 start_mongo_proc,
                                 start_cluster,
                                 kill_all,
                                 PORTS_ONE)
from tests.util import assert_soon
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect


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

        assert(start_cluster())
        cls.conn = MongoClient('localhost:%s' % PORTS_ONE['PRIMARY'],
                               replicaSet='demo-repl')
        cls.connector = Connector(
            address="%s:%s" % ('localhost', PORTS_ONE["PRIMARY"]),
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

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
        """
        self.assertEqual(len(self.connector.shard_set), 1)

    def test_initial(self):
        """Tests search and assures that the databases are clear.
        """
        self.conn['test']['test'].remove()
        self.synchronizer._delete()
        self.assertEqual(self.conn['test']['test'].find().count(), 0)
        self.assertEqual(len(self.synchronizer._search()), 0)

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

    def test_rollback(self):
        """Tests rollback. Rollback is performed by inserting one document,
            killing primary, inserting another doc, killing secondary,
            and then restarting both.
        """
        primary_conn = MongoClient('localhost', int(PORTS_ONE['PRIMARY']))

        self.conn['test']['test'].insert({'name': 'paul'})
        while self.conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)

        kill_mongo_proc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient('localhost', int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']

        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert,
                       {'name': 'pauline'})
        assert_soon(lambda: len(self.synchronizer._search()) == 2)
        result_set_1 = self.synchronizer._search()
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 2)
        for item in result_set_1:
            if item['name'] == 'pauline':
                self.assertEqual(item['_id'], result_set_2['_id'])
        kill_mongo_proc('localhost', PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log")
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log")

        time.sleep(2)
        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['name'], 'paul')
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)
                #self.assertEqual(conn['test']['test'].find().count(), 1)

    def test_stress(self):
        """Stress test the system by inserting a large number of docs.
        """
        for i in range(0, 100):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        while len(self.synchronizer._search()) != 100:
            time.sleep(5)
       # conn['test']['test'].create_index('name')
        for i in range(0, 100):
            result_set_1 = self.synchronizer._search()
            for item in result_set_1:
                if (item['name'] == 'Paul' + str(i)):
                    self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
        in global variable. Rollback is performed like before, but with more
            documents.
        """
        while len(self.synchronizer._search()) != 0:
            time.sleep(1)
        for i in range(0, 100):
            self.conn['test']['test'].insert(
                {'name': 'Paul ' + str(i)})

        while len(self.synchronizer._search()) != 100:
            time.sleep(1)
        primary_conn = MongoClient('localhost', int(PORTS_ONE['PRIMARY']))
        kill_mongo_proc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient('localhost', int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = -1
        while count + 1 < 100:
            try:
                count += 1
                self.conn['test']['test'].insert(
                    {'name': 'Pauline ' + str(count)})
            except (OperationFailure, AutoReconnect):
                time.sleep(1)
        while (len(self.synchronizer._search())
                != self.conn['test']['test'].find().count()):
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        i = 0
        for item in result_set_1:
            if 'Pauline' in item['name']:
                result_set_2 = self.conn['test']['test'].find_one(
                    {'name': item['name']})
                self.assertEqual(item['_id'], result_set_2['_id'])

        kill_mongo_proc('localhost', PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log")
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log")

        while (len(self.synchronizer._search()) != 100):
            time.sleep(5)

        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 100)
        for item in result_set_1:
            self.assertTrue('Paul' in item['name'])
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 100)


def abort_test(self):
    """ Aborts the test
    """
    sys.exit(1)


if __name__ == '__main__':
    unittest.main()
