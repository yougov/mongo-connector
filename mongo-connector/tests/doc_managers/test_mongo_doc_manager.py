
"""Tests each of the functions in mongo_doc_manager
"""

import unittest
import time
import sys
import inspect
import os

file = inspect.getfile(inspect.currentframe())
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))
doc_folder = cmd_folder.rsplit("/", 2)[0]
doc_folder += '/doc_managers'
if doc_folder not in sys.path:
    sys.path.insert(0, doc_folder)

mongo_folder = cmd_folder.rsplit("/", 2)[0]
if mongo_folder not in sys.path:
    sys.path.insert(0, mongo_folder)

from mongo_doc_manager import DocManager
from pymongo import Connection

MongoDoc = DocManager("localhost:30000")
mongo = Connection("localhost:30000")['test']['test']


class MongoDocManagerTester(unittest.TestCase):
    """Test class for MongoDocManager
    """

    def runTest(self):
        unittest.TestCase.__init__(self)

    def setUp(self):
        """Empty Mongo at the start of every test
        """
        mongo.remove()

    def test_upsert(self):
        """Ensure we can properly insert into Mongo via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        time.sleep(3)
        res = mongo.find()
        self.assertTrue(res.count() == 1)
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul', 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        time.sleep(1)
        res = mongo.find()
        self.assertTrue(res.count() == 1)
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')
        print("PASSED UPSERT")

    def test_remove(self):
        """Ensure we can properly delete from Mongo via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        time.sleep(3)
        res = mongo.find()
        self.assertTrue(res.count() == 1)

        MongoDoc.remove(docc)
        time.sleep(1)
        res = mongo.find()
        self.assertTrue(res.count() == 0)
        print("PASSED REMOVE")

    def test_full_search(self):
        """Query Mongo for all docs via API and via DocManager's
        _search(), compare.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul', 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        MongoDoc.commit()
        search = MongoDoc._search()
        search2 = list(mongo.find())
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])
        print("PASSED _SEARCH")

    def test_search(self):
        """Query Mongo for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """

        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865,
                'ns': 'test.test'}
        MongoDoc.upsert(docc)
        docc2 = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866,
                 'ns': 'test.test'}
        MongoDoc.upsert(docc2)
        docc3 = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870,
                 'ns': 'test.test'}
        MongoDoc.upsert(docc3)
        search = MongoDoc.search(5767301236327972865, 5767301236327972866)
        self.assertTrue(len(search) == 2)
        self.assertTrue(list(search)[0]['name'] == 'John')
        self.assertTrue(list(search)[1]['name'] == 'John Paul')
        print("PASSED SEARCH")

    def test_get_last_doc(self):
        """Insert documents, verify that get_last_doc() returns the one with
            the latest timestamp.
        """
        docc = {'_id': '4', 'name': 'Hare', '_ts': 3, 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': 2, 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        docc = {'_id': '6', 'name': 'Mr T.', '_ts': 1, 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        time.sleep(1)
        doc = MongoDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')
        docc = {'_id': '6', 'name': 'HareTwin', '_ts': 4, 'ns': 'test.test'}
        MongoDoc.upsert(docc)
        time.sleep(3)
        doc = MongoDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '6')
        print("PASSED GET LAST DOC")

if __name__ == '__main__':
    unittest.main()
