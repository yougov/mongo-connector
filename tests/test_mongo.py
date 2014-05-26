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
from tests import mongo_host, STRESS_COUNT
from tests.setup_cluster import (start_replica_set,
                                 kill_replica_set,
                                 start_mongo_proc,
                                 restart_mongo_proc,
                                 kill_mongo_proc)
from mongo_connector.doc_managers.mongo_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from pymongo.errors import OperationFailure, AutoReconnect
from tests.util import assert_soon


class TestSynchronizer(unittest.TestCase):
    """ Tests the mongo instance
    """

    @classmethod
    def setUpClass(cls):
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()
        cls.standalone_port = start_mongo_proc(options=['--nojournal',
                                                        '--noprealloc'])
        cls.mongo_doc = DocManager('%s:%d' % (mongo_host, cls.standalone_port))
        cls.mongo_doc._remove()
        _, cls.secondary_p, cls.primary_p = start_replica_set('test-mongo')
        cls.conn = MongoClient(mongo_host, cls.primary_p,
                               replicaSet='test-mongo')

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_mongo_proc(cls.standalone_port)
        kill_replica_set('test-mongo')

    def tearDown(self):
        self.connector.join()

    def setUp(self):
        self.connector = Connector(
            address='%s:%s' % (mongo_host, self.primary_p),
            oplog_checkpoint="config.txt",
            target_url='%s:%d' % (mongo_host, self.standalone_port),
            ns_set=['test.test'],
            u_key='_id',
            auth_key=None,
            doc_manager='mongo_connector/doc_managers/mongo_doc_manager.py'
        )
        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        self.conn['test']['test'].remove()
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 0)

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
            properly
        """

        self.assertEqual(len(self.connector.shard_set), 1)

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

    def test_update(self):
        """Test update operations."""
        # Insert
        self.conn.test.test.insert({"a": 0})
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 1)

        def check_update(update_spec):
            updated = self.conn.test.test.find_and_modify(
                {"a": 0},
                update_spec,
                new=True
            )
            # Allow some time for update to propagate
            time.sleep(2)
            replicated = self.mongo_doc.mongo.test.test.find_one({"a": 0})
            self.assertEqual(replicated, updated)

        # Update by adding a field
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
        """Tests rollback. We force a rollback by adding a doc, killing the
            primary, adding another doc, killing the new primary, and then
            restarting both.
        """
        primary_conn = MongoClient(mongo_host, self.primary_p)
        self.conn['test']['test'].insert({'name': 'paul'})
        condition = lambda: self.conn['test']['test'].find_one(
            {'name': 'paul'}) is not None
        assert_soon(condition)
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search()) == 1)

        kill_mongo_proc(self.primary_p, destroy=False)
        new_primary_conn = MongoClient(mongo_host, self.secondary_p)
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
        kill_mongo_proc(self.secondary_p, destroy=False)

        restart_mongo_proc(self.primary_p)
        assert_soon(
            lambda: primary_conn['admin'].command("isMaster")['ismaster'])

        restart_mongo_proc(self.secondary_p)

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

        for i in range(0, STRESS_COUNT):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        search = self.mongo_doc._search
        condition = lambda: sum(1 for _ in search()) == STRESS_COUNT
        assert_soon(condition)
        for i in range(0, STRESS_COUNT):
            result_set_1 = self.mongo_doc._search()
            for item in result_set_1:
                if(item['name'] == 'Paul' + str(i)):
                    self.assertEqual(item['_id'], item['_id'])

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
            in global variable. Strategy for rollback is the same as before.
        """

        for i in range(0, STRESS_COUNT):
            self.conn['test']['test'].insert({'name': 'Paul ' + str(i)})

        search = self.mongo_doc._search
        condition = lambda: sum(1 for _ in search()) == STRESS_COUNT
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
        assert_soon(lambda: sum(1 for _ in self.mongo_doc._search())
                    == self.conn['test']['test'].find().count())
        result_set_1 = self.mongo_doc._search()
        for item in result_set_1:
            if 'Pauline' in item['name']:
                result_set_2 = self.conn['test']['test'].find_one(
                    {'name': item['name']})
                self.assertEqual(item['_id'], result_set_2['_id'])

        kill_mongo_proc(self.secondary_p, destroy=False)

        restart_mongo_proc(self.primary_p)
        db_admin = primary_conn['admin']
        assert_soon(lambda: db_admin.command("isMaster")['ismaster'])
        restart_mongo_proc(self.secondary_p)

        search = self.mongo_doc._search
        condition = lambda: sum(1 for _ in search()) == STRESS_COUNT
        assert_soon(condition)

        result_set_1 = list(self.mongo_doc._search())
        self.assertEqual(len(result_set_1), STRESS_COUNT)
        for item in result_set_1:
            self.assertTrue('Paul' in item['name'])
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), STRESS_COUNT)


if __name__ == '__main__':
    unittest.main()
