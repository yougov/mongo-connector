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

"""Test Solr search using the synchronizer, i.e. as it would be used by an user
    """
import logging
import os
import time
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
                                 kill_all,
                                 PORTS_ONE)
from tests.util import wait_for
from pysolr import Solr, SolrError
from mongo_connector.connector import Connector
from pymongo.errors import OperationFailure, AutoReconnect
from requests.exceptions import MissingSchema


NUMBER_OF_DOC_DIRS = 100
HOSTNAME = os.environ.get('HOSTNAME', socket.gethostname())
MAIN_ADDR = os.environ.get('MAIN_ADDR', "27217")
CONFIG = os.environ.get('CONFIG', "config.txt")
PORTS_ONE['MAIN'] = MAIN_ADDR


class TestSynchronizer(unittest.TestCase):
    """ Tests Solr
    """

    def runTest(self):
        """ Runs tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):
        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        cls.flag = start_cluster()
        if cls.flag:
            cls.conn = MongoClient('%s:%s' % (HOSTNAME, PORTS_ONE['MAIN']))
            # Creating a Solr object with an invalid URL
            # doesn't create an exception
            cls.solr_conn = Solr('http://localhost:8983/solr')
            try:
                cls.solr_conn.commit()
            except (SolrError, MissingSchema):
                cls.err_msg = "Cannot connect to Solr!"
                cls.flag = False
            if cls.flag:
                cls.solr_conn.delete(q='*:*')
        else:
            cls.err_msg = "Shards cannot be added to mongos"

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def setUp(self):
        if not self.flag:
            self.fail(self.err_msg)

        self.connector = Connector(
            address=('%s:%s' % (HOSTNAME, PORTS_ONE['MAIN'])),
            oplog_checkpoint=CONFIG,
            target_url='http://localhost:8983/solr',
            ns_set=['test.test'],
            u_key='_id',
            auth_key=None,
            doc_manager='mongo_connector/doc_managers/solr_doc_manager.py'
        )
        self.connector.start()
        while len(self.connector.shard_set) == 0:
            time.sleep(1)
        count = 0
        while (True):
            try:
                self.conn['test']['test'].remove()
                break
            except (AutoReconnect, OperationFailure):
                time.sleep(1)
                count += 1
                if count > 60:
                    unittest.SkipTest('Call to remove failed too '
                                      'many times in setup')
        while (len(self.solr_conn.search('*:*')) != 0):
            time.sleep(1)

    def tearDown(self):
        self.connector.join()

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
        """

        self.assertEqual(len(self.connector.shard_set), 1)

    def test_initial(self):
        """Tests search and assures that the databases are clear.
        """

        while (True):
            try:
                self.conn['test']['test'].remove()
                break
            except OperationFailure:
                continue

        self.solr_conn.delete(q='*:*')
        self.assertEqual(self.conn['test']['test'].find().count(), 0)
        self.assertEqual(len(self.solr_conn.search('*:*')), 0)

    def test_insert(self):
        """Tests insert
        """

        self.conn['test']['test'].insert({'name': 'paulie'})
        while (len(self.solr_conn.search('*:*')) == 0):
            time.sleep(1)
        result_set_1 = self.solr_conn.search('paulie')
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """

        self.conn['test']['test'].remove({'name': 'paulie'})
        while (len(self.solr_conn.search('*:*')) == 1):
            time.sleep(1)
        result_set_1 = self.solr_conn.search('paulie')
        self.assertEqual(len(result_set_1), 0)

    def test_rollback(self):
        """Tests rollback. We force a rollback by inserting one doc, killing
            primary, adding another doc, killing the new primary, and
            restarting both the servers.
        """

        primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['PRIMARY']))

        self.conn['test']['test'].insert({'name': 'paul'})
        while self.conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)
        while len(self.solr_conn.search('*:*')) != 1:
            time.sleep(1)
        kill_mongo_proc(HOSTNAME, PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                self.conn['test']['test'].insert(
                    {'name': 'pauline'})
                break
            except OperationFailure:
                count += 1
                if count > 60:
                    self.fail('Call to insert failed too '
                              'many times in test_rollback')
                time.sleep(1)
                continue

        while (len(self.solr_conn.search('*:*')) != 2):
            time.sleep(1)

        result_set_1 = self.solr_conn.search('pauline')
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
        kill_mongo_proc(HOSTNAME, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log", None)

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log", None)

        time.sleep(2)
        result_set_1 = self.solr_conn.search('pauline')
        self.assertEqual(len(result_set_1), 0)
        result_set_2 = self.solr_conn.search('paul')
        self.assertEqual(len(result_set_2), 1)

    def test_stress(self):
        """Test stress by inserting and removing a large amount of docs.
        """
        #stress test
        for i in range(0, NUMBER_OF_DOC_DIRS):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        while (len(self.solr_conn.search('*:*', rows=NUMBER_OF_DOC_DIRS))
                != NUMBER_OF_DOC_DIRS):
            time.sleep(5)
        for i in range(0, NUMBER_OF_DOC_DIRS):
            result_set_1 = self.solr_conn.search('Paul ' + str(i))
            for item in result_set_1:
                self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
        in global variable. The rollback is performed the same way as before
            but with more docs
        """

        self.conn['test']['test'].remove()
        while len(self.solr_conn.search('*:*', rows=NUMBER_OF_DOC_DIRS)) != 0:
            time.sleep(1)
        for i in range(0, NUMBER_OF_DOC_DIRS):
            self.conn['test']['test'].insert(
                {'name': 'Paul ' + str(i)})

        while (len(self.solr_conn.search('*:*', rows=NUMBER_OF_DOC_DIRS))
                != NUMBER_OF_DOC_DIRS):
            time.sleep(1)
        primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['PRIMARY']))
        kill_mongo_proc(HOSTNAME, PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']

        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = -1
        while count + 1 < NUMBER_OF_DOC_DIRS:
            try:
                count += 1
                self.conn['test']['test'].insert(
                    {'name': 'Pauline ' + str(count)})

            except (OperationFailure, AutoReconnect):
                time.sleep(1)

        while (len(self.solr_conn.search('*:*', rows=NUMBER_OF_DOC_DIRS * 2)) !=
               self.conn['test']['test'].find().count()):
            time.sleep(1)
        result_set_1 = self.solr_conn.search(
            'Pauline',
            rows=NUMBER_OF_DOC_DIRS * 2, sort='_id asc'
        )
        for item in result_set_1:
            result_set_2 = self.conn['test']['test'].find_one(
                {'name': item['name']})
            self.assertEqual(item['_id'], str(result_set_2['_id']))

        kill_mongo_proc(HOSTNAME, PORTS_ONE['SECONDARY'])
        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log", None)

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log", None)

        while (len(self.solr_conn.search(
                'Pauline',
                rows=NUMBER_OF_DOC_DIRS * 2)) != 0):
            time.sleep(15)
        result_set_1 = self.solr_conn.search(
            'Pauline',
            rows=NUMBER_OF_DOC_DIRS * 2
        )
        self.assertEqual(len(result_set_1), 0)
        result_set_2 = self.solr_conn.search(
            'Paul',
            rows=NUMBER_OF_DOC_DIRS * 2
        )
        self.assertEqual(len(result_set_2), NUMBER_OF_DOC_DIRS)

    def test_valid_fields(self):
        """ Tests documents with field definitions
        """
        inserted_obj = self.conn['test']['test'].insert(
            {'name': 'test_valid'})
        self.conn['test']['test'].update(
            {'_id': inserted_obj},
            {'$set': {'popularity': 1}}
        )

        docman = self.connector.doc_managers[0]
        for _ in range(60):
            if len(docman._search("*:*")) != 0:
                break
            time.sleep(1)
        else:
            self.fail("Timeout when removing docs from Solr")

        result = docman.get_last_doc()
        self.assertIn('popularity', result)
        self.assertEqual(len(docman._search(
            "name=test_valid")), 1)

    def test_invalid_fields(self):
        """ Tests documents without field definitions
        """
        inserted_obj = self.conn['test']['test'].insert(
            {'name': 'test_invalid'})
        self.conn['test']['test'].update(
            {'_id': inserted_obj},
            {'$set': {'break_this_test': 1}}
        )

        docman = self.connector.doc_managers[0]
        for _ in range(60):
            if len(docman._search("*:*")) != 0:
                break
            time.sleep(1)
        else:
            self.fail("Timeout when removing docs from Solr")

        result = docman.get_last_doc()
        self.assertNotIn('break_this_test', result)
        self.assertEqual(len(docman._search(
            "name=test_invalid")), 1)

    def test_dynamic_fields(self):
        """ Tests dynamic field definitions

        The following fields are supplied in the provided schema.xml:
        <dynamicField name="*_i" type="int" indexed="true" stored="true"/>
        <dynamicField name="i_*" type="int" indexed="true" stored="true"/>

        Cases:
        1. Match on first definition
        2. Match on second definition
        3. No match
        """
        self.solr_conn.delete(q='*:*')

        match_first = {"_id": 0, "foo_i": 100}
        match_second = {"_id": 1, "i_foo": 200}
        match_none = {"_id": 2, "foo": 300}

        # Connector is already running
        self.conn["test"]["test"].insert(match_first)
        self.conn["test"]["test"].insert(match_second)
        self.conn["test"]["test"].insert(match_none)

        # Should have documents in Solr now
        self.assertTrue(wait_for(lambda: len(self.solr_conn.search("*:*")) > 0),
                        "Solr doc manager should allow dynamic fields")

        # foo_i and i_foo should be indexed, foo field should not exist
        self.assertEqual(len(self.solr_conn.search("foo_i:100")), 1)
        self.assertEqual(len(self.solr_conn.search("i_foo:200")), 1)

        # SolrError: "undefined field foo"
        logger = logging.getLogger("pysolr")
        logger.error("You should see an ERROR log message from pysolr here. "
                     "This indicates success, not an error in the test.")
        with self.assertRaises(SolrError):
            self.solr_conn.search("foo:300")

    def test_nested_fields(self):
        """Test indexing fields that are sub-documents in MongoDB

        The following fields are defined in the provided schema.xml:

        <field name="person.address.street" type="string" ... />
        <field name="person.address.state" type="string" ... />
        <dynamicField name="numbers.*" type="string" ... />
        <dynamicField name="characters.*" type="string" ... />

        """

        self.solr_conn.delete(q='*:*')

        # Connector is already running
        self.conn["test"]["test"].insert({
            "name": "Jeb",
            "billing": {
                "address": {
                    "street": "12345 Mariposa Street",
                    "state": "California"
                }
            }
        })
        self.conn["test"]["test"].insert({
            "numbers": ["one", "two", "three"],
            "characters": [
                {"name": "Big Bird",
                 "color": "yellow"},
                {"name": "Elmo",
                 "color": "red"},
                "Cookie Monster"
            ]
        })

        self.assertTrue(wait_for(lambda: len(self.solr_conn.search("*:*")) > 0),
                        "documents should have been replicated to Solr")

        # Search for first document
        results = self.solr_conn.search(
            "billing.address.street:12345\ Mariposa\ Street")
        self.assertEqual(len(results), 1)
        self.assertEqual(next(iter(results))["billing.address.state"],
                         "California")

        # Search for second document
        results = self.solr_conn.search(
            "characters.1.color:red")
        self.assertEqual(len(results), 1)
        self.assertEqual(next(iter(results))["numbers.2"], "three")
        results = self.solr_conn.search("characters.2:Cookie\ Monster")
        self.assertEqual(len(results), 1)

if __name__ == '__main__':
    unittest.main()
