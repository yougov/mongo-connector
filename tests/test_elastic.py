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
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

sys.path[0:0] = [""]

from elasticsearch import Elasticsearch
from gridfs import GridFS
from pymongo import MongoClient

from tests import elastic_pair, mongo_host, STRESS_COUNT
from tests.setup_cluster import (start_replica_set,
                                 kill_replica_set,
                                 restart_mongo_proc,
                                 kill_mongo_proc)
from mongo_connector.doc_managers.elastic_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect
from tests.util import assert_soon


class ElasticsearchTestCase(unittest.TestCase):
    """Base class for all ES TestCases."""

    @classmethod
    def setUpClass(cls):
        cls.elastic_conn = Elasticsearch(hosts=[elastic_pair])
        cls.elastic_doc = DocManager(elastic_pair,
                                     auto_commit_interval=0)

    def setUp(self):
        # Create target index in elasticsearch
        self.elastic_conn.indices.create(index='test.test')
        self.elastic_conn.cluster.health(wait_for_status='yellow',
                                         index='test.test')

    def tearDown(self):
        self.elastic_conn.indices.delete(index='test.test', ignore=404)

    def _search(self, query=None):
        query = query or {"match_all": {}}
        return self.elastic_doc._stream_search(
            index="test.test",
            body={"query": query}
        )

    def _count(self):
        return self.elastic_conn.count(index='test.test')['count']

    def _remove(self):
        self.elastic_conn.indices.delete_mapping(
            index="test.test",
            doc_type=self.elastic_doc.doc_type
        )
        self.elastic_conn.indices.refresh(index="test.test")


class TestElastic(ElasticsearchTestCase):
    """Integration tests for mongo-connector + Elasticsearch."""

    @classmethod
    def setUpClass(cls):
        """Start the cluster."""
        super(TestElastic, cls).setUpClass()
        _, cls.secondary_p, cls.primary_p = start_replica_set('test-elastic')
        cls.conn = MongoClient(mongo_host, cls.primary_p,
                               replicaSet='test-elastic')

    @classmethod
    def tearDownClass(cls):
        """Kill the cluster."""
        kill_replica_set('test-elastic')

    def tearDown(self):
        """Stop the Connector thread."""
        super(TestElastic, self).tearDown()
        self.connector.join()

    def setUp(self):
        """Start a new Connector for each test."""
        super(TestElastic, self).setUp()
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()
        docman = DocManager(elastic_pair)
        self.connector = Connector(
            address='%s:%s' % (mongo_host, self.primary_p),
            oplog_checkpoint='config.txt',
            ns_set=['test.test'],
            auth_key=None,
            doc_managers=(docman,),
            gridfs_set=['test.test']
        )

        self.conn.test.test.drop()
        self.conn.test.test.files.drop()
        self.conn.test.test.chunks.drop()

        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        assert_soon(lambda: self._count() == 0)

    def test_shard_length(self):
        self.assertEqual(len(self.connector.shard_set), 1)

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
        test_data = "test_insert_file test file"
        id = fs.put(test_data, filename="test.txt")
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
        id = fs.put("test file", filename="test.txt")
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

        # Update by changing a value within a sub-document (contains array)
        check_update({"$inc": {"b.0.c": 1}})

        # Update by changing the value within an array
        check_update({"$inc": {"b.1.f": 12}})

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
        primary_conn = MongoClient(mongo_host, self.primary_p)

        self.conn['test']['test'].insert({'name': 'paul'})
        condition1 = lambda: self.conn['test']['test'].find(
            {'name': 'paul'}).count() == 1
        condition2 = lambda: self._count() == 1
        assert_soon(condition1)
        assert_soon(condition2)

        kill_mongo_proc(self.primary_p, destroy=False)

        new_primary_conn = MongoClient(mongo_host, self.secondary_p)

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
        kill_mongo_proc(self.secondary_p, destroy=False)

        restart_mongo_proc(self.primary_p)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        restart_mongo_proc(self.secondary_p)

        time.sleep(2)
        result_set_1 = list(self._search())
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['name'], 'paul')
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)

    def test_stress(self):
        """Stress test for inserting and removing many documents."""
        for i in range(0, STRESS_COUNT):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        condition = lambda: self._count() == STRESS_COUNT
        assert_soon(condition)
        self.assertEqual(
            set('Paul ' + str(i) for i in range(STRESS_COUNT)),
            set(item['name'] for item in self._search())
        )

    def test_stressed_rollback(self):
        """Stress test for a rollback with many documents."""
        for i in range(0, STRESS_COUNT):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})

        condition = lambda: self._count() == STRESS_COUNT
        assert_soon(condition)
        primary_conn = MongoClient(mongo_host, self.primary_p)
        kill_mongo_proc(self.primary_p, destroy=False)

        new_primary_conn = MongoClient(mongo_host, self.secondary_p)

        admin = new_primary_conn['admin']
        assert_soon(lambda: admin.command("isMaster")['ismaster'])

        time.sleep(5)
        count = -1
        while count + 1 < STRESS_COUNT:
            try:
                count += 1
                self.conn['test']['test'].insert(
                    {'name': 'Pauline ' + str(count)})
            except (OperationFailure, AutoReconnect):
                time.sleep(1)
        assert_soon(lambda: self._count()
                    == self.conn['test']['test'].find().count())
        result_set_1 = self._search()
        for item in result_set_1:
            if 'Pauline' in item['name']:
                result_set_2 = self.conn['test']['test'].find_one(
                    {'name': item['name']})
                self.assertEqual(item['_id'], str(result_set_2['_id']))

        kill_mongo_proc(self.secondary_p, destroy=False)

        restart_mongo_proc(self.primary_p)
        db_admin = primary_conn["admin"]
        assert_soon(lambda: db_admin.command("isMaster")['ismaster'])
        restart_mongo_proc(self.secondary_p)

        search = self._search
        condition = lambda: sum(1 for _ in search()) == STRESS_COUNT
        assert_soon(condition)

        result_set_1 = list(self._search())
        self.assertEqual(len(result_set_1), STRESS_COUNT)
        for item in result_set_1:
            self.assertTrue('Paul' in item['name'])
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), STRESS_COUNT)


if __name__ == '__main__':
    unittest.main()
