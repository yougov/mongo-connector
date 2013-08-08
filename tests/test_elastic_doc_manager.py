"""Tests each of the functions in elastic_doc_manager
"""

import unittest
import time
import sys
import inspect
import os

sys.path[0:0] = [""]

from mongo_connector.doc_managers.elastic_doc_manager import DocManager
from pyes import ES, MatchAllQuery
from pyes.exceptions import (IndexMissingException,
                            NoServerAvailable,
                            TypeMissingException)


class elastic_docManagerTester(unittest.TestCase):
    """Test class for elastic_docManager
    """

    def runTest(self):
        """Runs all Tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):
        """Initializes ES DocManager and a direct connection to elastic_conn
        """
        cls.elastic_doc = DocManager("http://localhost:9200", auto_commit=False)
        cls.elastic_conn = ES(server="http://localhost:9200")

    def setUp(self):
        """Empty ElasticSearch at the start of every test
        """
        try:
            self.elastic_conn.delete('test.test', 'string', '')
        except (IndexMissingException, TypeMissingException,
                NoServerAvailable):
            pass

    def test_invalid_url(self):
        """Ensure DocManager fails for a bad Solr url.
        """
        #Invalid URL
        #Invalid URL
        count = 0
        try:
            DocManager("http://doesntexist.cskjdfhskdjfhdsom")
        except SystemError:
            count += 1
        self.assertTrue(count == 1)

    def test_upsert(self):
        """Ensure we can properly insert into ElasticSearch via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        self.elastic_doc.commit()
        res = self.elastic_conn.search(MatchAllQuery())
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

    def test_remove(self):
        """Ensure we can properly delete from ElasticSearch via DocManager.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        self.elastic_doc.commit()
        res = self.elastic_conn.search(MatchAllQuery())
        self.assertTrue(len(res) == 1)

        self.elastic_doc.remove(docc)
        self.elastic_doc.commit()
        res = self.elastic_conn.search(MatchAllQuery())
        self.assertTrue(len(res) == 0)

    def test_full_search(self):
        """Query ElasticSearch for all docs via API and via DocManager's
            _search(), compare.
        """

        docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul', 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        self.elastic_doc.commit()
        search = self.elastic_doc._search()
        search2 = self.elastic_conn.search(MatchAllQuery())
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])

    def test_search(self):
        """Query ElasticSearch for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """

        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865,
                'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc2 = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866,
                 'ns': 'test.test'}
        self.elastic_doc.upsert(docc2)
        docc3 = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870,
                 'ns': 'test.test'}
        self.elastic_doc.upsert(docc3)
        self.elastic_doc.commit()
        search = self.elastic_doc.search(
            5767301236327972865, 5767301236327972866)
        self.assertTrue(len(search) == 2)
        self.assertTrue(list(search)[0]['name'] == 'John')
        self.assertTrue(list(search)[1]['name'] == 'John Paul')
    
    def test_elastic_commit(self):
        """Test that documents get properly added to ElasticSearch.
        """

        docc = {'_id': '3', 'name': 'Waldo', 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        res = self.elastic_doc._search()
        assert(len(res) == 1)
        for result in res:
            assert(result['name'] == 'Waldo')

    def test_get_last_doc(self):
        """Insert documents, verify that get_last_doc() returns the one with
            the latest timestamp.
        """
        docc = {'_id': '4', 'name': 'Hare', '_ts': 3, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': 2, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc = {'_id': '6', 'name': 'Mr T.', '_ts': 1, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        self.assertEqual(self.elastic_doc.elastic.count()['count'], 3)
        doc = self.elastic_doc.get_last_doc()
        self.assertEqual(doc['_id'], '4')

        docc = {'_id': '6', 'name': 'HareTwin', '_ts': 4, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        doc = self.elastic_doc.get_last_doc()
        self.assertEqual(doc['_id'], '6')
        self.assertEqual(self.elastic_doc.elastic.count()['count'], 3)

if __name__ == '__main__':
    unittest.main()
