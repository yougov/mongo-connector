import unittest
import time
from solr_doc_manager import SolrDocManager
from pysolr import Solr

class SolrDocManagerTester(unittest.TestCase):

    def __init__(self):

        super(SolrDocManagerTester, self).__init__()
        self.solr = Solr("http://localhost:8080/solr/")

    def runTest(self):

        #Invalid URL
        s = SolrDocManager("http://doesntexist.cskjdfhskdjfhdsom")
        self.assertTrue(s.solr is None)

        #valid URL
        SolrDoc = SolrDocManager("http://localhost:8080/solr/")
        self.solr.delete(q ='*:*')


        #test upsert
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert([docc])
        self.solr.commit()
        res = self.solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')
        
        docc = {'_id': '1', 'name': 'Paul'}
        SolrDoc.upsert([docc])
        self.solr.commit()
        res = self.solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')

        #test remove
        SolrDoc.remove('1')
        self.solr.commit()
        res = self.solr.search('*:*')
        self.assertTrue(len(res) == 0)

        #test search
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert([docc])
        docc = {'_id': '2', 'name': 'Paul'}
        SolrDoc.upsert([docc])
        self.solr.commit()
        search = SolrDoc.search('*:*')
        search2 = self.solr.search('*:*')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0,len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])
                    
        #test solr commit
        docc = {'_id': '3', 'name': 'Waldo'}
        SolrDoc.upsert([docc])
        res = SolrDoc.search('Waldo')
        assert(len(res) == 0)
        time.sleep(1)
        res = SolrDoc.search('Waldo')
        assert(len(res) != 0)

        #test get last doc
        docc = {'_id': '4', 'name': 'Hare', 'ts': '2'}
        SolrDoc.upsert([docc])
        docc = {'_id': '5', 'name': 'Tortoise', 'ts': '1'}
        SolrDoc.upsert([docc])
        self.solr.commit()
        doc = SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')

        docc = {'_id': '6', 'name': 'HareTwin', 'ts':'2'}
        self.solr.commit()
        doc = SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4' or doc['_id'] == '6');

        

        
