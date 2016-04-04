# -*- encoding: utf-8 -*-
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
import sys
import time

from pysolr import Solr, SolrError

from bson import SON
from gridfs import GridFS

sys.path[0:0] = [""]

from mongo_connector.compat import u
from mongo_connector.connector import Connector
from mongo_connector.doc_managers.solr_doc_manager import DocManager
from mongo_connector.test_utils import ReplicaSet, solr_url, assert_soon
from mongo_connector.util import retry_until_ok
from tests import unittest


class SolrTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.solr_conn = Solr(solr_url)
        cls.docman = DocManager(solr_url, auto_commit_interval=0)

    def _search(self, query):
        return self.docman._stream_search(query)

    def _remove(self):
        self.solr_conn.delete(q="*:*", commit=True)


class TestSolr(SolrTestCase):
    """ Tests Solr
    """

    @classmethod
    def setUpClass(cls):
        SolrTestCase.setUpClass()
        cls.repl_set = ReplicaSet().start()
        cls.conn = cls.repl_set.client()

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        cls.repl_set.stop()

    def setUp(self):
        self._remove()
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        open("oplog.timestamp", "w").close()
        docman = DocManager(solr_url, auto_commit_interval=0)
        self.connector = Connector(
            mongo_address=self.repl_set.uri,
            ns_set=['test.test'],
            doc_managers=(docman,),
            gridfs_set=['test.test']
        )
        retry_until_ok(self.conn.test.test.drop)
        retry_until_ok(self.conn.test.test.files.drop)
        retry_until_ok(self.conn.test.test.chunks.drop)
        self._remove()
        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)

    def tearDown(self):
        self.connector.join()

    def test_insert(self):
        """Tests insert
        """

        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search('*:*')) > 0)
        result_set_1 = list(self.solr_conn.search('name:paulie'))
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """
        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 1)
        self.conn['test']['test'].delete_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 0)

    def test_insert_file(self):
        """Tests inserting a gridfs file
        """
        fs = GridFS(self.conn['test'], 'test')
        test_data = "test_insert_file test file"
        id = fs.put(test_data, filename="test.txt", encoding='utf8')
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search('*:*')) > 0)

        res = list(self.solr_conn.search('content:*test_insert_file*'))
        if not res:
            res = list(self.solr_conn.search('_text_:*test_insert_file*'))
        self.assertEqual(len(res), 1)
        doc = res[0]
        self.assertEqual(doc['filename'], "test.txt")
        self.assertEqual(doc['_id'], str(id))
        content = doc.get('content', doc.get('_text_', None))
        self.assertTrue(content)
        self.assertIn(test_data.strip(), content[0].strip())

    def test_remove_file(self):
        """Tests removing a gridfs file
        """
        fs = GridFS(self.conn['test'], 'test')
        id = fs.put("test file", filename="test.txt", encoding='utf8')
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 1)
        fs.delete(id)
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search("*:*")) == 0)

    def test_update(self):
        """Test update operations on Solr.

        Need to have the following defined in schema.xml:

        <field name="a" type="int" indexed="true" stored="true" />
        <field name="b.0.c" type="int" indexed="true" stored="true" />
        <field name="b.10.c" type="int" indexed="true" stored="true" />
        <field name="b.0.e" type="int" indexed="true" stored="true" />
        <field name="b.1.d" type="int" indexed="true" stored="true" />
        <field name="b.1.f" type="int" indexed="true" stored="true" />
        <field name="b.2.e" type="int" indexed="true" stored="true" />
        """
        docman = self.connector.doc_managers[0]

        # Use diabolical value for _id to test string escaping as well.
        self.conn.test.test.insert_one({"_id": u'+-&え|!(){}[]^"~*?:\\/', "a": 0})
        assert_soon(lambda: sum(1 for _ in self._search("*:*")) == 1)

        def check_update(update_spec):
            updated = self.conn.test.command(
                SON([('findAndModify', 'test'),
                     ('query', {"a": 0}),
                     ('update', update_spec),
                     ('new', True)]))['value']

            # Stringify _id to match what will be retrieved from Solr
            updated[u('_id')] = u(updated['_id'])
            # Flatten the MongoDB document to match Solr
            updated = docman._clean_doc(updated, 'dummy.namespace', 0)
            # Allow some time for update to propagate
            time.sleep(3)
            replicated = list(self._search("a:0"))[0]

            # Remove add'l fields until these are stored in a separate Solr core
            updated.pop('_ts')
            replicated.pop('_ts')
            updated.pop('ns')
            replicated.pop('ns')

            # Remove field added by Solr
            replicated.pop("_version_")

            self.assertEqual(replicated, updated)

        # Update by adding a field.
        # Note that Solr can't mix types within an array
        check_update({"$set": {"b": [{"c": 10}, {"d": 11}]}})

        # Update by setting an attribute of a sub-document beyond end of array.
        check_update({"$set": {"b.10.c": 42}})

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

        # Update by adding new bucket to list
        check_update({"$push": {"b": {"e": 12}}})

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

        primary_conn = self.repl_set.primary.client()

        self.conn['test']['test'].insert_one({'name': 'paul'})
        assert_soon(
            lambda: self.conn.test.test.find({'name': 'paul'}).count() == 1)
        assert_soon(
            lambda: sum(1 for _ in self.solr_conn.search('*:*')) == 1)
        self.repl_set.primary.stop(destroy=False)

        new_primary_conn = self.repl_set.secondary.client()
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert_one,
                       {'name': 'pauline'})
        assert_soon(lambda: sum(1 for _ in self.solr_conn.search('*:*')) == 2)

        result_set_1 = list(self.solr_conn.search('name:pauline'))
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()

        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        self.repl_set.secondary.start()

        time.sleep(2)
        result_set_1 = self.solr_conn.search('name:pauline')
        self.assertEqual(sum(1 for _ in result_set_1), 0)
        result_set_2 = self.solr_conn.search('name:paul')
        self.assertEqual(sum(1 for _ in result_set_2), 1)

    def test_valid_fields(self):
        """ Tests documents with field definitions
        """
        inserted_obj = self.conn['test']['test'].insert_one(
            {'name': 'test_valid'}).inserted_id
        self.conn['test']['test'].update_one(
            {'_id': inserted_obj},
            {'$set': {'popularity': 1}}
        )

        docman = self.connector.doc_managers[0]
        assert_soon(lambda: sum(1 for _ in self._search("*:*")) > 0)
        result = docman.get_last_doc()
        self.assertIn('popularity', result)
        self.assertEqual(sum(1 for _ in self._search(
            "name:test_valid")), 1)

    def test_invalid_fields(self):
        """ Tests documents without field definitions
        """
        inserted_obj = self.conn['test']['test'].insert_one(
            {'name': 'test_invalid'}).inserted_id
        self.conn['test']['test'].update_one(
            {'_id': inserted_obj},
            {'$set': {'break_this_test': 1}}
        )

        docman = self.connector.doc_managers[0]
        assert_soon(lambda: sum(1 for _ in self._search("*:*")) > 0)

        result = docman.get_last_doc()
        self.assertNotIn('break_this_test', result)
        self.assertEqual(sum(1 for _ in self._search(
            "name:test_invalid")), 1)

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
        self.conn["test"]["test"].insert_one(match_first)
        self.conn["test"]["test"].insert_one(match_second)
        self.conn["test"]["test"].insert_one(match_none)

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

        <field name="billing.address.street" type="string" ... />
        <field name="billing.address.state" type="string" ... />
        <dynamicField name="numbers.*" type="string" ... />
        <dynamicField name="characters.*" type="string" ... />

        """

        # Connector is already running
        self.conn["test"]["test"].insert_one({
            "name": "Jeb",
            "billing": {
                "address": {
                    "street": "12345 Mariposa Street",
                    "state": "California"
                }
            }
        })
        self.conn["test"]["test"].insert_one({
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
