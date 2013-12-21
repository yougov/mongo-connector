
"""Tests each of the functions in mongo_doc_manager
"""

import unittest
import time
import sys
import inspect
import os

sys.path[0:0] = [""]

from mongo_connector.doc_managers.mongo_doc_manager import DocManager
try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection    
from tests.setup_cluster import start_single_mongod_instance, kill_mongo_proc


class MongoDocManagerTester(unittest.TestCase):
    """Test class for MongoDocManager
    """

    def runTest(self):
        """ Runs the tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):    
        start_single_mongod_instance("30000", "/MC", "MC_log")
        cls.MongoDoc = DocManager("localhost:30000")
        cls.mongo = Connection("localhost:30000")['test']['test']

    @classmethod
    def tearDownClass(cls):        
        kill_mongo_proc('localhost', 30000)

    def setUp(self):
        """Empty Mongo at the start of every test
        """
        self.mongo.remove()

    def test_upsert(self):
        """Ensure we can properly insert into Mongo via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        time.sleep(3)
        res = self.mongo.find()
        self.assertTrue(res.count() == 1)
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul', 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        time.sleep(1)
        res = self.mongo.find()
        self.assertTrue(res.count() == 1)
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')

    def test_remove(self):
        """Ensure we can properly delete from Mongo via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        time.sleep(3)
        res = self.mongo.find()
        self.assertTrue(res.count() == 1)

        self.MongoDoc.remove(docc)
        time.sleep(1)
        res = self.mongo.find()
        self.assertTrue(res.count() == 0)

    def test_full_search(self):
        """Query Mongo for all docs via API and via DocManager's
        _search(), compare.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul', 'ns': 'test.test'}
        self.MongoDoc.upsert(docc)
        self.MongoDoc.commit()
        search = list(self.MongoDoc._search())
        search2 = list(self.mongo.find())
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(search[i] == search2[i])

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
        self.assertTrue(search[0]['name'] == 'John')
        self.assertTrue(search[1]['name'] == 'John Paul')

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

if __name__ == '__main__':
    unittest.main()
