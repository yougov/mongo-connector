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

from gridfs import GridFS
from pymongo import MongoClient

from tests import solr_pair, mongo_host, STRESS_COUNT
from tests.setup_cluster import (start_replica_set,
                                 kill_replica_set,
                                 restart_mongo_proc,
                                 kill_mongo_proc)
from tests.util import assert_soon
from pysolr import Solr, SolrError
from mongo_connector.connector import Connector
from mongo_connector.doc_managers.solr_doc_manager import DocManager
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect


class TestSynchronizer(unittest.TestCase):
    """ Tests Solr
    """

    @classmethod
    def setUpClass(cls):
        _, cls.secondary_p, cls.primary_p = start_replica_set('test-solr')
        cls.conn = MongoClient(mongo_host, cls.primary_p,
                               replicaSet='test-solr')
        cls.solr_conn = Solr('http://%s/solr' % solr_pair)
        cls.solr_conn.delete(q='*:*')

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_replica_set('test-solr')

    def setUp(self):
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()
        docman = DocManager('http://%s/solr' % solr_pair,
                            auto_commit_interval=0)
        self.connector = Connector(
            address='%s:%s' % (mongo_host, self.primary_p),
            oplog_checkpoint='config.txt',
            ns_set=['test.test'],
            auth_key=None,
            doc_managers=(docman,),
            gridfs_set=['test.test']
        )
        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        retry_until_ok(self.conn.test.test.remove)
        retry_until_ok(self.conn.test.test.files.remove)
        retry_until_ok(self.conn.test.test.chunks.remove)
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search('*:*')) == 0)

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
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search('*:*')) > 0)
        result_set_1 = list(self.solr_conn.search('paulie'))
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """
        self.conn['test']['test'].insert({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 1)
        self.conn['test']['test'].remove({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 0)

    def test_insert_file(self):
        """Tests inserting a gridfs file
        """
        fs = GridFS(self.conn['test'], 'test')
        test_data = "test_insert_file test file"
        id = fs.put(test_data, filename="test.txt")
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search('*:*')) > 0)

        res = list(self.solr_conn.search('test_insert_file'))
        self.assertEqual(len(res), 1)
        doc = res[0]
        self.assertEqual(doc['filename'], "test.txt")
        self.assertEqual(doc['_id'], str(id))
        self.assertEqual(doc['content'][0].strip(), test_data.strip())

    def test_remove_file(self):
        """Tests removing a gridfs file
        """
        fs = GridFS(self.conn['test'], 'test')
        id = fs.put("test file", filename="test.txt")
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 1)
        fs.delete(id)
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 0)

    def test_update(self):
        """Test update operations on Solr.

        Need to have the following defined in schema.xml:

        <field name="a" type="int" indexed="true" stored="true" />
        <field name="b.0.c" type="int" indexed="true" stored="true" />
        <field name="b.0.e" type="int" indexed="true" stored="true" />
        <field name="b.1.d" type="int" indexed="true" stored="true" />
        <field name="b.1.f" type="int" indexed="true" stored="true" />
        """
        docman = self.connector.doc_managers[0]

        # Insert
        self.conn.test.test.insert({"a": 0})
        assert_soon(lambda: sum(1 for _ in docman._search("*:*")) == 1)

        def check_update(update_spec):
            updated = self.conn.test.test.find_and_modify(
                {"a": 0},
                update_spec,
                new=True
            )
            # Stringify _id to match what will be retrieved from Solr
            updated['_id'] = str(updated['_id'])
            # Flatten the MongoDB document to match Solr
            updated = docman._clean_doc(updated)
            # Allow some time for update to propagate
            time.sleep(1)
            replicated = list(docman._search("a:0"))[0]
            # Remove add'l fields until these are stored in a separate Solr core
            replicated.pop("_ts")
            replicated.pop("ns")
            # Remove field added by Solr
            replicated.pop("_version_")
            self.assertEqual(replicated, docman._clean_doc(updated))

        # Update by adding a field.
        # Note that Solr can't mix types within an array
        check_update({"$set": {"b": [{"c": 10}, {"d": 11}]}})

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

        # Update by replacing an entire sub-document
        check_update({"$set": {"b.0": {"e": 4}}})

        # Update by adding a sub-document
        check_update({"$set": {"b": {"0": {"c": 100}}}})

        # Update whole document
        check_update({"a": 0, "b": {"1": {"d": 10000}}})

    def test_rollback(self):
        """Tests rollback. We force a rollback by inserting one doc, killing
            primary, adding another doc, killing the new primary, and
            restarting both the servers.
        """

        primary_conn = MongoClient(mongo_host, self.primary_p)

        self.conn['test']['test'].insert({'name': 'paul'})
        assert_soon(
            lambda: self.conn.test.test.find({'name': 'paul'}).count() == 1)
        assert_soon(
            lambda: sum(1 for _ in self.solr_conn.search('*:*')) == 1)
        kill_mongo_proc(self.primary_p, destroy=False)

        new_primary_conn = MongoClient(mongo_host, self.secondary_p)
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert,
                       {'name': 'pauline'})
        assert_soon(
            lambda: sum(1 for _ in self.solr_conn.search('*:*')) == 2)

        result_set_1 = list(self.solr_conn.search('pauline'))
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
        kill_mongo_proc(self.secondary_p, destroy=False)

        restart_mongo_proc(self.primary_p)

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        restart_mongo_proc(self.secondary_p)

        time.sleep(2)
        result_set_1 = self.solr_conn.search('pauline')
        self.assertEqual(sum(1 for _ in result_set_1), 0)
        result_set_2 = self.solr_conn.search('paul')
        self.assertEqual(sum(1 for _ in result_set_2), 1)

    def test_stress(self):
        """Test stress by inserting and removing a large amount of docs.
        """
        #stress test
        for i in range(0, STRESS_COUNT):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        assert_soon(
            lambda: sum(1 for _ in self.solr_conn.search(
                '*:*', rows=STRESS_COUNT)) == STRESS_COUNT)
        for i in range(0, STRESS_COUNT):
            result_set_1 = self.solr_conn.search('Paul ' + str(i))
            for item in result_set_1:
                self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with a large number of documents"""

        for i in range(0, STRESS_COUNT):
            self.conn['test']['test'].insert(
                {'name': 'Paul ' + str(i)})

        assert_soon(
            lambda: sum(1 for _ in self.solr_conn.search(
                '*:*', rows=STRESS_COUNT)) == STRESS_COUNT)
        primary_conn = MongoClient(mongo_host, self.primary_p)
        kill_mongo_proc(self.primary_p, destroy=False)

        new_primary_conn = MongoClient(mongo_host, self.secondary_p)
        admin_db = new_primary_conn['admin']

        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = -1
        while count + 1 < STRESS_COUNT:
            try:
                count += 1
                self.conn['test']['test'].insert(
                    {'name': 'Pauline ' + str(count)})

            except (OperationFailure, AutoReconnect):
                time.sleep(1)

        collection_size = self.conn['test']['test'].find().count()
        assert_soon(
            lambda: sum(1 for _ in self.solr_conn.search(
                '*:*', rows=STRESS_COUNT * 2)) == collection_size)
        result_set_1 = self.solr_conn.search(
            'Pauline',
            rows=STRESS_COUNT * 2, sort='_id asc'
        )
        for item in result_set_1:
            result_set_2 = self.conn['test']['test'].find_one(
                {'name': item['name']})
            self.assertEqual(item['_id'], str(result_set_2['_id']))

        kill_mongo_proc(self.secondary_p, destroy=False)
        restart_mongo_proc(self.primary_p)

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        restart_mongo_proc(self.secondary_p)

        assert_soon(lambda: sum(1 for _ in self.solr_conn.search(
            'Pauline', rows=STRESS_COUNT * 2)) == 0)
        result_set_1 = list(self.solr_conn.search(
            'Pauline',
            rows=STRESS_COUNT * 2
        ))
        self.assertEqual(len(result_set_1), 0)
        result_set_2 = list(self.solr_conn.search(
            'Paul',
            rows=STRESS_COUNT * 2
        ))
        self.assertEqual(len(result_set_2), STRESS_COUNT)

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
        assert_soon(lambda: sum(1 for _ in docman._search("*:*")) > 0)
        result = docman.get_last_doc()
        self.assertIn('popularity', result)
        self.assertEqual(sum(1 for _ in docman._search(
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
        assert_soon(lambda: sum(1 for _ in docman._search("*:*")) > 0)

        result = docman.get_last_doc()
        self.assertNotIn('break_this_test', result)
        self.assertEqual(sum(1 for _ in docman._search(
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
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) > 0,
                    "Solr doc manager should allow dynamic fields")

        # foo_i and i_foo should be indexed, foo field should not exist
        self.assertEqual(sum(1 for _ in self.solr_conn.search("foo_i:100")), 1)
        self.assertEqual(sum(1 for _ in self.solr_conn.search("i_foo:200")), 1)

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

        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) > 0,
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
