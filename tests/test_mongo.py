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

import os
import sys
import time

from bson import SON
from gridfs import GridFS

sys.path[0:0] = [""]

from mongo_connector.doc_managers.mongo_doc_manager import DocManager
from mongo_connector.connector import Connector
from mongo_connector.util import retry_until_ok
from mongo_connector.test_utils import (ReplicaSet,
                                        Server,
                                        connector_opts,
                                        assert_soon,
                                        close_client)
from tests import unittest


class MongoTestCase(unittest.TestCase):

    use_single_meta_collection = False

    @classmethod
    def setUpClass(cls):
        cls.standalone = Server().start()
        cls.mongo_doc = DocManager(cls.standalone.uri)
        cls.mongo_conn = cls.standalone.client()
        cls.mongo = cls.mongo_conn['test']['test']

    @classmethod
    def tearDownClass(cls):
        close_client(cls.mongo_conn)
        cls.standalone.stop()

    def _search(self, **kwargs):
        for doc in self.mongo.find(**kwargs):
            yield doc

        fs = GridFS(self.mongo_conn['test'], 'test')

        collection_name = 'test.test'
        if self.use_single_meta_collection:
            collection_name = '__oplog'
        for doc in self.mongo_conn['__mongo_connector'][collection_name].find():
            if doc.get('gridfs_id'):
                for f in fs.find({'_id': doc['gridfs_id']}):
                    doc['filename'] = f.filename
                    doc['content'] = f.read()
                    yield doc

    def _remove(self):
        self.mongo_conn['test']['test'].drop()
        self.mongo_conn['test']['test.files'].drop()
        self.mongo_conn['test']['test.chunks'].drop()


class TestMongo(MongoTestCase):
    """ Tests the mongo instance
    """

    @classmethod
    def setUpClass(cls):
        MongoTestCase.setUpClass()
        cls.repl_set = ReplicaSet().start()
        cls.conn = cls.repl_set.client()

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        MongoTestCase.tearDownClass()
        cls.repl_set.stop()

    def tearDown(self):
        self.connector.join()

    def setUp(self):
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        self._remove()
        self.connector = Connector(
            mongo_address=self.repl_set.uri,
            ns_set=['test.test'],
            doc_managers=(self.mongo_doc,),
            gridfs_set=['test.test'],
            **connector_opts
        )

        self.conn.test.test.drop()
        self.conn.test.test.files.drop()
        self.conn.test.test.chunks.drop()

        self.connector.start()
        assert_soon(lambda: len(self.connector.shard_set) > 0)
        assert_soon(lambda: sum(1 for _ in self._search()) == 0)

    def test_insert(self):
        """Tests insert
        """

        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)
        result_set_1 = self._search()
        self.assertEqual(sum(1 for _ in result_set_1), 1)
        result_set_2 = self.conn['test']['test'].find_one()
        for item in result_set_1:
            self.assertEqual(item['_id'], result_set_2['_id'])
            self.assertEqual(item['name'], result_set_2['name'])

    def test_remove(self):
        """Tests remove
        """

        self.conn['test']['test'].insert_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)
        self.conn['test']['test'].delete_one({'name': 'paulie'})
        assert_soon(lambda: sum(1 for _ in self._search()) != 1)
        self.assertEqual(sum(1 for _ in self._search()), 0)

    def test_insert_file(self):
        """Tests inserting a gridfs file
        """
        fs = GridFS(self.conn['test'], 'test')
        test_data = b"test_insert_file test file"
        id = fs.put(test_data, filename="test.txt", encoding='utf8')
        assert_soon(lambda: sum(1 for _ in self._search()) > 0)

        res = list(self._search())
        self.assertEqual(len(res), 1)
        doc = res[0]
        self.assertEqual(doc['filename'], 'test.txt')
        self.assertEqual(doc['_id'], id)
        self.assertEqual(doc['content'], test_data)

    def test_remove_file(self):
        fs = GridFS(self.conn['test'], 'test')
        id = fs.put("test file", filename="test.txt", encoding='utf8')
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)
        fs.delete(id)
        assert_soon(lambda: sum(1 for _ in self._search()) == 0)

    def test_update(self):
        """Test update operations."""
        # Insert
        self.conn.test.test.insert_one({"a": 0})
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)

        def check_update(update_spec):
            updated = self.conn.test.command(
                SON([('findAndModify', 'test'),
                     ('query', {"a": 0}),
                     ('update', update_spec),
                     ('new', True)]))['value']

            def update_worked():
                replicated = self.mongo_doc.mongo.test.test.find_one({"a": 0})
                return replicated == updated

            # Allow some time for update to propagate
            assert_soon(update_worked)

        # Update by adding a field
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
        """Tests rollback. We force a rollback by adding a doc, killing the
        primary, adding another doc, killing the new primary, and then
        restarting both.
        """
        primary_conn = self.repl_set.primary.client()
        self.conn['test']['test'].insert_one({'name': 'paul'})
        condition = lambda: self.conn['test']['test'].find_one(
            {'name': 'paul'}) is not None
        assert_soon(condition)
        assert_soon(lambda: sum(1 for _ in self._search()) == 1)

        self.repl_set.primary.stop(destroy=False)
        new_primary_conn = self.repl_set.secondary.client()
        admin = new_primary_conn['admin']
        condition = lambda: admin.command("isMaster")['ismaster']
        assert_soon(lambda: retry_until_ok(condition))

        retry_until_ok(self.conn.test.test.insert_one,
                       {'name': 'pauline'})
        assert_soon(lambda: sum(1 for _ in self._search()) == 2)
        result_set_1 = list(self._search())
        result_set_2 = self.conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(result_set_1), 2)
        # make sure pauline is there
        for item in result_set_1:
            if item['name'] == 'pauline':
                self.assertEqual(item['_id'], result_set_2['_id'])
        self.repl_set.secondary.stop(destroy=False)

        self.repl_set.primary.start()
        assert_soon(
            lambda: primary_conn['admin'].command("isMaster")['ismaster'])

        self.repl_set.secondary.start()

        time.sleep(2)
        result_set_1 = list(self._search())
        self.assertEqual(len(result_set_1), 1)
        for item in result_set_1:
            self.assertEqual(item['name'], 'paul')
        find_cursor = retry_until_ok(self.conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)


if __name__ == '__main__':
    unittest.main()
