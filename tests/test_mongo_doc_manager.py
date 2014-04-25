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

"""Tests each of the functions in mongo_doc_manager
"""

import time
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

sys.path[0:0] = [""]

from mongo_connector.doc_managers.mongo_doc_manager import DocManager
from pymongo import MongoClient

from tests import mongo_host
from tests.setup_cluster import start_mongo_proc, kill_mongo_proc


class MongoDocManagerTester(unittest.TestCase):
    """Test class for MongoDocManager
    """

    @classmethod
    def setUpClass(cls):
        cls.standalone_port = start_mongo_proc(options=['--nojournal',
                                                        '--noprealloc'])
        cls.standalone_pair = '%s:%d' % (mongo_host, cls.standalone_port)
        cls.MongoDoc = DocManager(cls.standalone_pair)
        cls.mongo_conn = MongoClient(cls.standalone_pair)
        cls.mongo = cls.mongo_conn['test']['test']

        cls.namespaces_inc = ["test.test_include1", "test.test_include2"]
        cls.namespaces_exc = ["test.test_exclude1", "test.test_exclude2"]
        cls.choosy_docman = DocManager(
            cls.standalone_pair,
            namespace_set=MongoDocManagerTester.namespaces_inc
        )

    @classmethod
    def tearDownClass(cls):
        kill_mongo_proc(cls.standalone_port)

    def setUp(self):
        """Empty Mongo at the start of every test
        """

        self.mongo_conn.drop_database("__mongo_connector")
        self.mongo.remove()

        conn = MongoClient('%s:%d' % (mongo_host, self.standalone_port))
        for ns in self.namespaces_inc + self.namespaces_exc:
            db, coll = ns.split('.', 1)
            conn[db][coll].remove()

    def test_namespaces(self):
        """Ensure that a DocManager instantiated with a namespace set
        has the correct namespaces
        """

        self.assertEqual(set(self.namespaces_inc),
                         set(self.choosy_docman._namespaces()))

    def test_upsert(self):
        """Ensure we can properly insert into Mongo via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test',
                '_ts': 5767301236327972865}
        self.MongoDoc.upsert(docc)
        time.sleep(3)
        res = self.mongo.find()
        self.assertTrue(res.count() == 1)
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul', 'ns': 'test.test',
                '_ts': 5767301236327972865}
        self.MongoDoc.upsert(docc)
        time.sleep(1)
        res = self.mongo.find()
        self.assertTrue(res.count() == 1)
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')

    def test_remove(self):
        """Ensure we can properly delete from Mongo via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test',
                '_ts': 5767301236327972865}
        self.MongoDoc.upsert(docc)
        time.sleep(3)
        res = self.mongo.find()
        self.assertTrue(res.count() == 1)
        if "ns" not in docc:
            docc["ns"] = 'test.test'

        self.MongoDoc.remove(docc)
        time.sleep(1)
        res = self.mongo.find()
        self.assertTrue(res.count() == 0)

    def test_full_search(self):
        """Query Mongo for all docs via API and via DocManager's
        _search(), compare.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test',
                '_ts': 5767301236327972865}
        self.MongoDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul', 'ns': 'test.test',
                '_ts': 5767301236327972865}
        self.MongoDoc.upsert(docc)
        self.MongoDoc.commit()
        search = list(self.MongoDoc._search())
        search2 = list(self.mongo.find())
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        self.assertTrue(all(x in search for x in search2) and
                        all(y in search2 for y in search))

    def test_search(self):
        """Query Mongo for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """

        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865,
                'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        docc2 = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866,
                 'ns': 'test.test'}
        self.MongoDoc.upsert(docc2)
        docc3 = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870,
                 'ns': 'test.test'}
        self.MongoDoc.upsert(docc3)
        search = list(self.MongoDoc.search(5767301236327972865,
                                           5767301236327972866))
        self.assertTrue(len(search) == 2)
        result_id = [result.get("_id") for result in search]
        self.assertIn('1', result_id)
        self.assertIn('2', result_id)

    def test_search_namespaces(self):
        """Test search within timestamp range with a given namespace set
        """

        for ns in self.namespaces_inc + self.namespaces_exc:
            for i in range(100):
                self.choosy_docman.upsert({"_id": i, "ns": ns, "_ts": i})

        results = list(self.choosy_docman.search(0, 49))
        self.assertEqual(len(results), 100)
        for r in results:
            self.assertIn(r["ns"], self.namespaces_inc)

    def test_get_last_doc(self):
        """Insert documents, verify that get_last_doc() returns the one with
            the latest timestamp.
        """
        docc = {'_id': '4', 'name': 'Hare', '_ts': 3, 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': 2, 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        docc = {'_id': '6', 'name': 'Mr T.', '_ts': 1, 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        time.sleep(1)
        doc = self.MongoDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')
        docc = {'_id': '6', 'name': 'HareTwin', '_ts': 4, 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        time.sleep(3)
        doc = self.MongoDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '6')

    def test_get_last_doc_namespaces(self):
        """Ensure that get_last_doc returns the latest document in one of
        the given namespaces
        """

        # latest document is not in included namespace
        for i in range(100):
            ns = (self.namespaces_inc, self.namespaces_exc)[i % 2][0]
            self.choosy_docman.upsert({
                "_id": i,
                "ns": ns,
                "_ts": i
            })
        last_doc = self.choosy_docman.get_last_doc()
        self.assertEqual(last_doc["ns"], self.namespaces_inc[0])
        self.assertEqual(last_doc["_id"], 98)

        # remove latest document so last doc is in included namespace,
        # shouldn't change result
        db, coll = self.namespaces_inc[0].split(".", 1)
        MongoClient(self.standalone_pair)[db][coll].remove({"_id": 99})
        last_doc = self.choosy_docman.get_last_doc()
        self.assertEqual(last_doc["ns"], self.namespaces_inc[0])
        self.assertEqual(last_doc["_id"], 98)

if __name__ == '__main__':
    unittest.main()
