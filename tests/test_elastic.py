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

"""Test elastic search using the synchronizer, i.e. as it would be used by an
    user
"""
import time
import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
import socket

sys.path[0:0] = [""]

from pymongo import MongoClient

from tests.setup_cluster import (kill_mongo_proc,
                                 start_mongo_proc,
                                 start_cluster,
                                 kill_all)
from bson.dbref import DBRef
from bson.objectid import ObjectId
from bson.code import Code
from mongo_connector.doc_managers.elastic_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect
from tests.util import wait_for

PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27217"}
NUMBER_OF_DOC_DIRS = 100
HOSTNAME = os.environ.get('HOSTNAME', socket.gethostname())
PORTS_ONE['MONGOS'] = os.environ.get('MAIN_ADDR', "27217")
CONFIG = os.environ.get('CONFIG', "config.txt")


class TestElastic(unittest.TestCase):
    """ Tests the Elastic instance
    """

    def runTest(self):
        """ Runs the tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):
        """ Starts the cluster
        """
        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        cls.elastic_doc = DocManager('localhost:9200')
        cls.elastic_doc._remove()
        cls.flag = start_cluster()
        if cls.flag:
            cls.conn = MongoClient('%s:%s' % (HOSTNAME, PORTS_ONE['MONGOS']))

        import logging
        logger = logging.getLogger()
        loglevel = logging.INFO
        logger.setLevel(loglevel)

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def tearDown(self):
        """ Ends the connector
        """
        self.connector.join()

    def setUp(self):
        """ Starts a new connector for every test
        """
        if not self.flag:
            self.fail("Shards cannot be added to mongos")
        self.connector = Connector(
            address='%s:%s' % (HOSTNAME, PORTS_ONE['MONGOS']),
            oplog_checkpoint=CONFIG,
            target_url='localhost:9200',
            ns_set=['test.test'],
            u_key='_id',
            auth_key=None,
            doc_manager='mongo_connector/doc_managers/elastic_doc_manager.py'
        )
        self.connector.start()
        while len(self.connector.shard_set) == 0:
            pass
        self.conn['test']['test'].remove()
        wait_for(lambda : sum(1 for _ in self.elastic_doc._search()) == 0)

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
        self.assertEqual(sum(1 for _ in self.elastic_doc._search()), 0)

    def test_insert(self):
        """Tests insert
        """

        self.conn['test']['test'].insert({'name': 'paulie'})
        wait_for(lambda : sum(1 for _ in self.elastic_doc._search()) > 0)
        result_set_1 = list(self.elastic_doc._search())
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """

        self.conn['test']['test'].insert({'name': 'paulie'})
        wait_for(lambda : sum(1 for _ in self.elastic_doc._search()) == 1)
        self.conn['test']['test'].remove({'name': 'paulie'})
        wait_for(lambda : sum(1 for _ in self.elastic_doc._search()) != 1)
        self.assertEqual(sum(1 for _ in self.elastic_doc._search()), 0)

    def test_rollback(self):
        """Tests rollback. We force a rollback by adding a doc, killing the
            primary, adding another doc, killing the new primary, and then
            restarting both.
        """

        primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['PRIMARY']))

        self.conn['test']['test'].insert({'name': 'paul'})
        condition1 = lambda : self.conn['test']['test'].find(
            {'name': 'paul'}).count() == 1
        condition2 = lambda : sum(1 for _ in self.elastic_doc._search()) == 1
        wait_for(condition1)
        wait_for(condition2)

        kill_mongo_proc(HOSTNAME, PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['SECONDARY']))

        admin = new_primary_conn['admin']
        wait_for(lambda : admin.command("isMaster")['ismaster'])
        time.sleep(5)

        count = 0
        while True:
            try:
                self.conn['test']['test'].insert({'name': 'pauline'})
                break
            except OperationFailure:
                time.sleep(1)
                count += 1
                if count >= 60:
                    sys.exit(1)
                continue
        wait_for(lambda : sum(1 for _ in self.elastic_doc._search()) == 2)
        result_set_1 = list(self.elastic_doc._search())
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 2)
        #make sure pauline is there
        for item in result_set_1:
            if item['name'] == 'pauline':
                self.assertEqual(item['_id'], str(result_set_2['_id']))
        kill_mongo_proc(HOSTNAME, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        time.sleep(2)
        result_set_1 = list(self.elastic_doc._search())
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

        for i in range(0, NUMBER_OF_DOC_DIRS):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        search = self.elastic_doc._search
        condition = lambda : sum(1 for _ in search()) == NUMBER_OF_DOC_DIRS
        wait_for(condition)
        for i in range(0, NUMBER_OF_DOC_DIRS):
            result_set_1 = self.elastic_doc._search()
            for item in result_set_1:
                if(item['name'] == 'Paul' + str(i)):
                    self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
            in global variable. Strategy for rollback is the same as before.
        """

        for i in range(0, NUMBER_OF_DOC_DIRS):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})

        search = self.elastic_doc._search
        condition = lambda : sum(1 for _ in search()) == NUMBER_OF_DOC_DIRS
        wait_for(condition)
        primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['PRIMARY']))
        kill_mongo_proc(HOSTNAME, PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['SECONDARY']))

        admin = new_primary_conn['admin']
        wait_for(lambda : admin.command("isMaster")['ismaster'])

        time.sleep(5)
        count = -1
        while count + 1 < NUMBER_OF_DOC_DIRS:
            try:
                count += 1
                self.conn['test']['test'].insert(
                    {'name': 'Pauline ' + str(count)})
            except (OperationFailure, AutoReconnect):
                time.sleep(1)
        wait_for(lambda : sum(1 for _ in self.elastic_doc._search())
                      == self.conn['test']['test'].find().count())
        result_set_1 = self.elastic_doc._search()
        for item in result_set_1:
            if 'Pauline' in item['name']:
                result_set_2 = self.conn['test']['test'].find_one(
                    {'name': item['name']})
                self.assertEqual(item['_id'], str(result_set_2['_id']))

        kill_mongo_proc(HOSTNAME, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        db_admin = primary_conn["admin"]
        wait_for(lambda : db_admin.command("isMaster")['ismaster'])
        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        search = self.elastic_doc._search
        condition = lambda : sum(1 for _ in search()) == NUMBER_OF_DOC_DIRS
        wait_for(condition)

        result_set_1 = list(self.elastic_doc._search())
        self.assertEqual(len(result_set_1), NUMBER_OF_DOC_DIRS)
        for item in result_set_1:
            self.assertTrue('Paul' in item['name'])
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), NUMBER_OF_DOC_DIRS)

    def test_non_standard_fields(self):
        """ Tests ObjectIds, DBrefs, etc
        """
        # This test can break if it attempts to insert before the dump takes
        # place- this prevents it (other tests affected too actually)
        while (self.connector.shard_set['demo-repl'].checkpoint is None):
            time.sleep(1)
        docs = [
            {'foo': [1, 2]},
            {'bar': {'hello': 'world'}},
            {'code': Code("function x() { return 1; }")},
            {'dbref': {'_ref': DBRef('simple',
                ObjectId('509b8db456c02c5ab7e63c34'))}}
        ]
        try:
            self.conn['test']['test'].insert(docs)
        except OperationFailure:
            self.fail("Cannot insert documents into Elastic!")

        search = self.elastic_doc._search
        if not wait_for(lambda : sum(1 for _ in search()) == len(docs)):
            self.fail("Did not get all expected documents")
        self.assertIn("dbref", self.elastic_doc.get_last_doc())


def abort_test():
    """Aborts the test
    """
    sys.exit(1)

if __name__ == '__main__':
    unittest.main()
