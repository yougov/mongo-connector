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

sys.path[0:0] = [""]

from pymongo import MongoClient

from tests.setup_cluster import (kill_mongo_proc,
                                 start_mongo_proc,
                                 start_cluster,
                                 kill_all,
                                 PORTS_ONE)
from tests.util import assert_soon
from pysolr import Solr, SolrError
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect


class TestSynchronizer(unittest.TestCase):
    """ Tests Solr
    """

    @classmethod
    def setUpClass(cls):
        assert(start_cluster())
        cls.conn = MongoClient('localhost:%s' % PORTS_ONE['PRIMARY'],
                               replicaSet='demo-repl')
        cls.solr_conn = Solr('http://localhost:8983/solr')
        cls.solr_conn.delete(q='*:*')

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def setUp(self):
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()
        self.connector = Connector(
            address=('localhost:%s' % PORTS_ONE['PRIMARY']),
            oplog_checkpoint='config.txt',
            target_url='http://localhost:8983/solr',
            ns_set=['test.test'],
            u_key='_id',
            auth_key=None,
            doc_manager='mongo_connector/doc_managers/solr_doc_manager.py'
        )
        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        retry_until_ok(self.conn.test.test.remove)
        assert_soon(lambda: len(self.solr_conn.search('*:*')) == 0)

    def tearDown(self):
        self.connector.join()

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
        """

        self.assertEqual(len(self.connector.shard_set), 1)

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
        self.conn['test']['test'].insert({'name': 'paulie'})
        assert_soon(lambda: len(self.solr_conn.search("*:*")) == 1)
        self.conn['test']['test'].remove({'name': 'paulie'})
        assert_soon(lambda: len(self.solr_conn.search("*:*")) == 0)

    def test_rollback(self):
        """Tests rollback. We force a rollback by inserting one doc, killing
            primary, adding another doc, killing the new primary, and
            restarting both the servers.
        """

        primary_conn = MongoClient('localhost', int(PORTS_ONE['PRIMARY']))

        self.conn['test']['test'].insert({'name': 'paul'})
        while self.conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)
        while len(self.solr_conn.search('*:*')) != 1:
            time.sleep(1)
        kill_mongo_proc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient('localhost', int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert,
                       {'name': 'pauline'})
        while (len(self.solr_conn.search('*:*')) != 2):
            time.sleep(1)

        result_set_1 = self.solr_conn.search('pauline')
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
        kill_mongo_proc('localhost', PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log")

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log")

        time.sleep(2)
        result_set_1 = self.solr_conn.search('pauline')
        self.assertEqual(len(result_set_1), 0)
        result_set_2 = self.solr_conn.search('paul')
        self.assertEqual(len(result_set_2), 1)

    def test_stress(self):
        """Test stress by inserting and removing a large amount of docs.
        """
        #stress test
        for i in range(0, 100):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        while (len(self.solr_conn.search('*:*', rows=100))
                != 100):
            time.sleep(5)
        for i in range(0, 100):
            result_set_1 = self.solr_conn.search('Paul ' + str(i))
            for item in result_set_1:
                self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with a large number of documents"""

        for i in range(0, 100):
            self.conn['test']['test'].insert(
                {'name': 'Paul ' + str(i)})

        while (len(self.solr_conn.search('*:*', rows=100))
                != 100):
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

        while (len(self.solr_conn.search('*:*', rows=100 * 2)) !=
               self.conn['test']['test'].find().count()):
            time.sleep(1)
        result_set_1 = self.solr_conn.search(
            'Pauline',
            rows=100 * 2, sort='_id asc'
        )
        for item in result_set_1:
            result_set_2 = self.conn['test']['test'].find_one(
                {'name': item['name']})
            self.assertEqual(item['_id'], str(result_set_2['_id']))

        kill_mongo_proc('localhost', PORTS_ONE['SECONDARY'])
        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "replset1a",
                         "replset1a.log")

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "replset1b",
                         "replset1b.log")

        while (len(self.solr_conn.search(
                'Pauline',
                rows=100 * 2)) != 0):
            time.sleep(15)
        result_set_1 = self.solr_conn.search(
            'Pauline',
            rows=100 * 2
        )
        self.assertEqual(len(result_set_1), 0)
        result_set_2 = self.solr_conn.search(
            'Paul',
            rows=100 * 2
        )
        self.assertEqual(len(result_set_2), 100)

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
        assert_soon(lambda: len(self.solr_conn.search("*:*")) > 0,
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

        assert_soon(lambda: len(self.solr_conn.search("*:*")) > 0,
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
