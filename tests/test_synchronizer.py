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

"""Test synchronizer using DocManagerSimulator
"""
import os
import sys
import inspect
import socket

sys.path[0:0] = [""]

try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection    

import time
import unittest
from tests.setup_cluster import (kill_mongo_proc,
                                start_mongo_proc,
                                start_cluster,
                                kill_all)
from threading import Timer
from mongo_connector.mongo_connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect

PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27117"}
NUMBER_OF_DOC_DIRS = 100
HOSTNAME = os.environ.get('HOSTNAME', socket.gethostname())
PORTS_ONE['MONGOS'] = os.environ.get('MAIN_ADDR', "27217")
CONFIG = os.environ.get('CONFIG', "config.txt")

class TestSynchronizer(unittest.TestCase):
    """ Tests the synchronizers
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
        if PORTS_ONE['MONGOS'] != "27217":
            use_mongos = False

        cls.flag =  start_cluster(use_mongos=use_mongos)
        if cls.flag:
            cls.conn = Connection('%s:%s' % (HOSTNAME, PORTS_ONE['MONGOS']),
                              replicaSet="demo-repl")
            timer = Timer(60, abort_test)
            cls.connector = Connector("%s:%s" % (HOSTNAME, PORTS_ONE["MONGOS"]),
                CONFIG, None, ['test.test'], '_id', None, None)
            cls.synchronizer = cls.connector.doc_manager
            timer.start()
            cls.connector.start()
            while len(cls.connector.shard_set) == 0:
                pass
            timer.cancel()

    @classmethod
    def tearDownClass(cls):
        """ Tears down connector
        """
        cls.connector.join()
        kill_all()

    def setUp(self):
        """ Clears the db
        """
        if not self.flag:
            self.fail("Shards cannot be added to mongos")

        self.conn['test']['test'].remove(safe=True)
        while (len(self.synchronizer._search()) != 0):
            time.sleep(1)

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
        """
        self.assertEqual(len(self.connector.shard_set), 1)

    def test_initial(self):
        """Tests search and assures that the databases are clear.
        """
        self.conn['test']['test'].remove(safe=True)
        self.synchronizer._delete()
        self.assertEqual(self.conn['test']['test'].find().count(), 0)
        self.assertEqual(len(self.synchronizer._search()), 0)

    def test_insert(self):
        """Tests insert
        """
        self.conn['test']['test'].insert({'name': 'paulie'}, safe=True)
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

        self.conn['test']['test'].insert({'name': 'paulie'}, safe=True)
        while (len(self.synchronizer._search()) != 1):
            time.sleep(1)
        self.conn['test']['test'].remove({'name': 'paulie'}, safe=True)

        while (len(self.synchronizer._search()) == 1):
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), 0)

    def test_rollback(self):
        """Tests rollback. Rollback is performed by inserting one document,
            killing primary, inserting another doc, killing secondary,
            and then restarting both.
        """
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE['PRIMARY']))

        self.conn['test']['test'].insert({'name': 'paul'}, safe=True)
        while self.conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)

        kill_mongo_proc(HOSTNAME, PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']

        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                self.conn['test']['test'].insert({'name': 'pauline'}, safe=True)
                break
            except OperationFailure:
                count += 1
                if count > 60:
                    self.fail('Call to insert failed too'
                    ' many times in test_rollback')
                time.sleep(1)
                continue
        while (len(self.synchronizer._search()) != 2):
            time.sleep(1)
        result_set_1 = self.synchronizer._search()
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 2)
        for item in result_set_1:
            if item['name'] == 'pauline':
                self.assertEqual(item['_id'], result_set_2['_id'])
        kill_mongo_proc(HOSTNAME, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

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
        for i in range(0, NUMBER_OF_DOC_DIRS):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        while len(self.synchronizer._search()) != NUMBER_OF_DOC_DIRS:
            time.sleep(5)
       # conn['test']['test'].create_index('name')
        for i in range(0, NUMBER_OF_DOC_DIRS):
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
        for i in range(0, NUMBER_OF_DOC_DIRS):
            self.conn['test']['test'].insert(
                {'name': 'Paul ' + str(i)}, safe=True)

        while len(self.synchronizer._search()) != NUMBER_OF_DOC_DIRS:
            time.sleep(1)
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE['PRIMARY']))
        kill_mongo_proc(HOSTNAME, PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = -1
        while count + 1 < NUMBER_OF_DOC_DIRS:
            try:
                count += 1
                self.conn['test']['test'].insert({'name': 'Pauline ' 
                    + str(count)}, safe=True)
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

        kill_mongo_proc(HOSTNAME, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        while (len(self.synchronizer._search()) != NUMBER_OF_DOC_DIRS):
            time.sleep(5)

        result_set_1 = self.synchronizer._search()
        self.assertEqual(len(result_set_1), NUMBER_OF_DOC_DIRS)
        for item in result_set_1:
            self.assertTrue('Paul' in item['name'])
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), NUMBER_OF_DOC_DIRS)


def abort_test(self):
    """ Aborts the test
    """
    sys.exit(1)


if __name__ == '__main__':
    unittest.main()
