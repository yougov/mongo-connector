import unittest
import time
import sys
from doc_manager import DocManager
from pysolr import Solr

SolrDoc = DocManager("http://localhost:8080/solr/")
solr = Solr("http://localhost:8080/solr/")


class SolrDocManagerTester(unittest.TestCase):

    def runTest(self):
		unittest.TestCase.__init__(self)

    def setUp(self):
        solr.delete(q = '*:*')
    
    def test_invalid_URL(self):

        #Invalid URL
        s = DocManager("http://doesntexist.cskjdfhskdjfhdsom")
        self.assertTrue(s.solr is None)
        print 'PASSED INVALID URL'

    def test_upsert(self):
        #test upsert
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert(docc)
        solr.commit()
        res = solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')
        
        docc = {'_id': '1', 'name': 'Paul'}
        SolrDoc.upsert(docc)
        solr.commit()
        res = solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')
        print 'PASSED UPSERT'

    def test_remove(self):
        #test remove
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.remove(docc)
        solr.commit()
        res = solr.search('*:*')
        self.assertTrue(len(res) == 0)
        print 'PASSED REMOVE'

        
    def test__search(self):
        #test _search
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul'}
        SolrDoc.upsert(docc)
        solr.commit()
        search = SolrDoc._search('*:*')
        search2 = solr.search('*:*')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0,len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])
        print 'PASSED _SEARCH'
    
    def test_search(self):
        #test search
        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865}
        SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866}
        SolrDoc.upsert(docc)
        docc = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870}
        SolrDoc.upsert(docc)
        solr.commit()
        search = SolrDoc.search(5767301236327972865, 5767301236327972866)
        search2 = solr.search('John')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0,len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])
        print 'PASSED SEARCH'    

         
    def test_solr_commit(self):
        #test solr commit
        docc = {'_id': '3', 'name': 'Waldo'}
        SolrDoc.upsert(docc)
        res = SolrDoc._search('Waldo')
        assert(len(res) == 0)
        time.sleep(2)
        res = SolrDoc._search('Waldo')
        assert(len(res) != 0)
        print 'PASSED COMMIT'
        SolrDoc.auto_commit = False

    def test_get_last_doc(self):
        #test get last doc
        docc = {'_id': '4', 'name': 'Hare', '_ts': '2'}
        SolrDoc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': '1'}
        SolrDoc.upsert(docc)
        solr.commit()
        doc = SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')

        docc = {'_id': '6', 'name': 'HareTwin', 'ts':'2'}
        solr.commit()
        doc = SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4' or doc['_id'] == '6');
        print 'PASSED GET LAST DOC'

if __name__ == '__main__':
    unittest.main()

        

        
