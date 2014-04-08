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

"""Test mongo using the synchronizer, i.e. as it would be used by an
    user
"""
import time
import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

sys.path[0:0] = [""]

from pymongo import MongoClient
from tests.setup_cluster import (kill_mongo_proc,
                                 kill_all,
                                 start_mongo_proc,
                                 start_cluster,
                                 start_single_mongod_instance,
                                 PORTS_ONE)
from mongo_connector.doc_managers.mongo_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect
from tests.util import assert_soon


class TestSynchronizer(unittest.TestCase):
    """ Tests the mongo instance
    """

    def runTest(self):
        """ Runs the tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()
        start_single_mongod_instance("30000", "MC", "MC_log")
        cls.mongo_doc = DocManager("localhost:30000")
        cls.mongo_doc._remove()
        assert(start_cluster())
        cls.conn = MongoClient("localhost:%s" % PORTS_ONE['PRIMARY'],
                               replicaSet="demo-repl")

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_mongo_proc('localhost', 30000)
        kill_all()

    def tearDown(self):
        self.connector.join()

    def setUp(self):
        self.connector = Connector(
            address="localhost:%s" % PORTS_ONE["PRIMARY"],
            oplog_checkpoint="config.txt",
            target_url='localhost:30000',
            ns_set=['test.test'],
            u_key='_id',
            auth_key=None,
            doc_manager='mongo_connector/doc_managers/mongo_doc_manager.py'
        )
        self.connector.start()
        while len(self.connector.shard_set) == 0:
            pass
        self.conn['test']['test'].remove()
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 0)

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
            properly
        """

        self.assertEqual(len(self.connector.shard_set), 1)

    def test_initial(self):
        """Tests search and assures that the databases are clear.
        """

        self.conn['test']['test'].remove()
        self.assertEqual(self.conn['test']['test'].find().count(), 0)
        self.assertEqual(sum(1 for _ in self.mongo_doc._search()), 0)

    def test_insert(self):
        """Tests insert
        """

        self.conn['test']['test'].insert({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 1)
        result_set_1 = self.mongo_doc._search()
        self.assertEqual(sum(1 for _ in result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], result_set_2['_id'])
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """

        self.conn['test']['test'].insert({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 1)
        self.conn['test']['test'].remove({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) != 1)
        self.assertEqual(sum(1 for _ in self.mongo_doc._search()), 0)

    def test_rollback(self):
        """Tests rollback. We force a rollback by adding a doc, killing the
            primary, adding another doc, killing the new primary, and then
            restarting both.
        """
        primary_conn = MongoClient('localhost', int(PORTS_ONE['PRIMARY']))
        self.conn['test']['test'].insert({'name': 'paul'})
        condition = lambda: self.conn['test']['test'].find_one(
            {'name': 'paul'}) is not None
        assert_soon(condition)
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 1)

        kill_mongo_proc('localhost', PORTS_ONE['PRIMARY'])
        new_primary_conn = MongoClient('localhost', int(PORTS_ONE['SECONDARY']))
        admin = new_primary_conn['admin']
        condition = lambda: admin.command("isMaster")['ismaster']
        assert_soon(lambda: retry_until_ok(condition))

        retry_until_ok(self.conn.test.test.insert,
                       {'name': 'pauline'})
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 2)
        result_set_1 = list(self.mongo_doc._search())
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 2)
        #make sure pauline is there
        for item in result_set_1:
            if item['name'] == 'pauline':
                self.assertEqual(item['_id'], result_set_2['_id'])
        kill_mongo_proc('localhost', PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log")
        assert_soon(
            lambda: primary_conn['admin'].command("isMaster")['ismaster'])

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log")

        time.sleep(2)
        result_set_1 = list(self.mongo_doc._search())
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['name'], 'paul')
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)

    def test_stress(self):
        """Test stress by inserting and removing the number of documents
            specified in global
            variable
        """

        for i in range(0, 100):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        search = self.mongo_doc._search
        condition = lambda: sum(1 for _ in search()) == 100
        assert_soon(condition)
        for i in range(0, 100):
            result_set_1 = self.mongo_doc._search()
            for item in result_set_1:
                if(item['name'] == 'Paul' + str(i)):
                    self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
            in global variable. Strategy for rollback is the same as before.
        """

        for i in range(0, 100):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})

        search = self.mongo_doc._search
        condition = lambda: sum(1 for _ in search()) == 100
        assert_soon(condition)
        primary_conn = MongoClient('localhost', int(PORTS_ONE['PRIMARY']))
        kill_mongo_proc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient('localhost', int(PORTS_ONE['SECONDARY']))

        admin = new_primary_conn['admin']
        assert_soon(lambda: admin.command("isMaster")['ismaster'])

        time.sleep(5)
        count = -1
        while count + 1 < 100:
            try:
                count += 1
                self.conn['test']['test'].insert(
                    {'name': 'Pauline ' + str(count)})
            except (OperationFailure, AutoReconnect):
                time.sleep(1)
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search())
                    == self.conn['test']['test'].find().count())
        result_set_1 = self.mongo_doc._search()
        for item in result_set_1:
            if 'Pauline' in item['name']:
                result_set_2 = self.conn['test']['test'].find_one(
                    {'name': item['name']})
                self.assertEqual(item['_id'], result_set_2['_id'])

        kill_mongo_proc('localhost', PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log")
        db_admin = primary_conn['admin']
        assert_soon(lambda: db_admin.command("isMaster")['ismaster'])
        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log")

        search = self.mongo_doc._search
        condition = lambda: sum(1 for _ in search()) == 100
        assert_soon(condition)

        result_set_1 = list(self.mongo_doc._search())
        self.assertEqual(len(result_set_1), 100)
        for item in result_set_1:
            self.assertTrue('Paul' in item['name'])
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 100)


if __name__ == '__main__':
    unittest.main()
