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

"""Integration tests for mongo-connector + Elasticsearch."""
import base64
import os
import sys
import time

from elasticsearch import Elasticsearch
from gridfs import GridFS

sys.path[0:0] = [""]

from tests import elastic_pair
from tests.setup_cluster import ReplicaSet
from mongo_connector.doc_managers.elastic_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from tests.util import assert_soon
from tests import unittest


class ElasticsearchTestCase(unittest.TestCase):
    """Base class for all ES TestCases."""

    @classmethod
    def setUpClass(cls):
        cls.elastic_conn = Elasticsearch(hosts=[elastic_pair])
        cls.elastic_doc = DocManager(elastic_pair,
                                     auto_commit_interval=0)

    def setUp(self):
        # Create target index in elasticsearch
        self.elastic_conn.indices.create(index='test', ignore=400)
        self.elastic_conn.cluster.health(wait_for_status='yellow',
                                         index='test')

    def tearDown(self):
        self.elastic_conn.indices.delete(index='test', ignore=404)

    def _search(self, query=None):
        query = query or {"match_all": {}}
        return self.elastic_doc._stream_search(
            index="test", doc_type='test',
            body={"query": query}
        )

    def _count(self):
        return self.elastic_conn.count(index='test')['count']

    def _remove(self):
        self.elastic_conn.indices.delete_mapping(
            index="test", doc_type='test'
        )
        self.elastic_conn.indices.refresh(index="test")

    def _mappings(self, index='_all'):
        mappings = self.elastic_conn.indices.get_mapping(index=index)
        if index in mappings:
            return list(mappings[index]['mappings'].keys())
        return []

    def _indices(self):
        return list(self.elastic_conn.indices.stats()['indices'].keys())


class TestElastic(ElasticsearchTestCase):
    """Integration tests for mongo-connector + Elasticsearch."""

    @classmethod
    def setUpClass(cls):
        """Start the cluster."""
        super(TestElastic, cls).setUpClass()
        cls.repl_set = ReplicaSet().start()
        cls.conn = cls.repl_set.client()

    @classmethod
    def tearDownClass(cls):
        """Kill the cluster."""
        cls.repl_set.stop()

    def tearDown(self):
        """Stop the Connector thread."""
        super(TestElastic, self).tearDown()
        self.connector.join()

    def setUp(self):
        """Start a new Connector for each test."""
        super(TestElastic, self).setUp()
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        docman = DocManager(elastic_pair)
        self.connector = Connector(
            mongo_address=self.repl_set.uri,
            ns_set=['test.test'],
            doc_managers=(docman,),
            gridfs_set=['test.test']
        )

        self.conn.test.test.drop()
        self.conn.test.test.files.drop()
        self.conn.test.test.chunks.drop()

        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        assert_soon(lambda: self._count() == 0)

    def test_insert(self):
        """Test insert operations."""
        self.conn['test']['test'].insert({'name': 'paulie'})
        assert_soon(lambda: self._count() > 0)
        result_set_1 = list(self._search())
        self.assertEqual(len(result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], str(result_set_2['_id']))
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove operations."""
        self.conn['test']['test'].insert({'name': 'paulie'})
        assert_soon(lambda: self._count() == 1)
        self.conn['test']['test'].remove({'name': 'paulie'})
        assert_soon(lambda: self._count() != 1)
        self.assertEqual(self._count(), 0)

    def test_insert_file(self):
        """Tests inserting a gridfs file
        """
        fs = GridFS(self.conn['test'], 'test')
        test_data = b"test_insert_file test file"
        id = fs.put(test_data, filename="test.txt", encoding='utf8')
        assert_soon(lambda: self._count() > 0)

        query = {"match": {"_all": "test_insert_file"}}
        res = list(self._search(query))
        self.assertEqual(len(res), 1)
        doc = res[0]
        self.assertEqual(doc['filename'], 'test.txt')
        self.assertEqual(doc['_id'], str(id))
        self.assertEqual(base64.b64decode(doc['content']), test_data)

    def test_remove_file(self):
        fs = GridFS(self.conn['test'], 'test')
        id = fs.put("test file", filename="test.txt", encoding='utf8')
        assert_soon(lambda: self._count() == 1)
        fs.delete(id)
        assert_soon(lambda: self._count() == 0)

    def test_update(self):
        """Test update operations."""
        # Insert
        self.conn.test.test.insert({"a": 0})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)

        def check_update(update_spec):
            updated = self.conn.test.test.find_and_modify(
                {"a": 0},
                update_spec,
                new=True
            )
            # Stringify _id to match what will be retrieved from ES
            updated['_id'] = str(updated['_id'])
            # Allow some time for update to propagate
            time.sleep(1)
            replicated = next(self._search())
            self.assertEqual(replicated, updated)

        # Update by adding a field. Note that ES can't mix types within an array
        check_update({"$set": {"b": [{"c": 10}, {"d": 11}]}})

        # Update by setting an attribute of a sub-document beyond end of array.
        check_update({"$set": {"b.10.c": 42}})

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

        # Update by adding new bucket to list
        check_update({"$push": {"b": {"e": 12}}})

        # Update by changing an entire sub-document
        check_update({"$set": {"b.0": {"e": 4}}})

        # Update by adding a sub-document
        check_update({"$set": {"b": {"0": {"c": 100}}}})

        # Update whole document
        check_update({"a": 0, "b": {"1": {"d": 10000}}})

    def test_rollback(self):
        """Test behavior during a MongoDB rollback.

        We force a rollback by adding a doc, killing the primary,
        adding another doc, killing the new primary, and then
        restarting both.
        """
        primary_conn = self.repl_set.primary.client()

        self.conn['test']['test'].insert({'name': 'paul'})
        condition1 = lambda: self.conn['test']['test'].find(
            {'name': 'paul'}).count() == 1
        condition2 = lambda: self._count() == 1
        assert_soon(condition1)
        assert_soon(condition2)

        self.repl_set.primary.stop(destroy=False)

        new_primary_conn = self.repl_set.secondary.client()

        admin = new_primary_conn['admin']
        assert_soon(lambda: admin.command("isMaster")['ismaster'])
        time.sleep(5)
        retry_until_ok(self.conn.test.test.insert,
                       {'name': 'pauline'})
        assert_soon(lambda: self._count() == 2)
        result_set_1 = list(self._search())
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 2)
        #make sure pauline is there
        for item in result_set_1:
            if item['name'] == 'pauline':
                self.assertEqual(item['_id'], str(result_set_2['_id']))
        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        self.repl_set.secondary.start()

        time.sleep(2)
        result_set_1 = list(self._search())
        self.assertEqual(len(result_set_1), 1)

        for item in result_set_1:
            self.assertEqual(item['name'], 'paul')
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)

    def test_bad_int_value(self):
        self.conn.test.test.insert({
            'inf': float('inf'), 'nan': float('nan'),
            'still_exists': True})
        assert_soon(lambda: self._count() > 0)
        for doc in self._search():
            self.assertNotIn('inf', doc)
            self.assertNotIn('nan', doc)
            self.assertTrue(doc['still_exists'])

if __name__ == '__main__':
    unittest.main()
