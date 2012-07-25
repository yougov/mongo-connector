"""Tests each of the functions in elastic_doc_manager
"""

import unittest
import time
import sys
from doc_manager import DocManager
from pyes import ES, ESRange, RangeQuery, MatchAllQuery

ElasticDoc = DocManager("http://localhost:9200", auto_commit = False)
elastic = ES(server = "http://localhost:9200")

class ElasticDocManagerTester(unittest.TestCase):
	
	def runTest(self):
		unittest.TestCase.__init__(self)
		
    
	def setUp(self):
		try:
			elastic.delete('test.test', 'string', '')
		except:
			pass
        

	def test_invalid_URL(self):
    
       	#Invalid URL
		e = DocManager("http://doesntexistqwertasg.com")
		self.assertTrue(e.elastic is None)
		print ('PASSED INVALID URL')
		

	def test_upsert(self):
		#test upsert
		docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		ElasticDoc.commit()
		res = elastic.search(MatchAllQuery())
		for doc in res:
			self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')
        
		docc = {'_id': '1', 'name': 'Paul', 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		ElasticDoc.commit()
		res = elastic.search(MatchAllQuery())
		for doc in res:
			self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')
		print 'PASSED UPSERT'

            
	def test__search(self):
	    #test _search
		docc = {'_id': '1', 'name': 'John', 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		docc = {'_id': '2', 'name': 'Paul', 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		ElasticDoc.commit()
		search = ElasticDoc._search()
		search2 = elastic.search(MatchAllQuery())
		self.assertTrue(len(search) == len(search2))
		self.assertTrue(len(search) != 0)
		for i in range(0,len(search)):
			self.assertTrue(list(search)[i] == list(search2)[i])
		print 'PASSED _SEARCH'
	
            
	def test_search(self):
	    #test search
		docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865, 'ns':'test.test'}
		ElasticDoc.upsert(docc)
		docc2 = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866, 'ns':'test.test'}
		ElasticDoc.upsert(docc2)
		docc3 = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870, 'ns': 'test.test'}
		ElasticDoc.upsert(docc3)
		ElasticDoc.commit()
		search = ElasticDoc.search(5767301236327972865, 5767301236327972866)
		self.assertTrue(len(search) == 2)
		self.assertTrue (list(search)[0]['name']=='John')
		self.assertTrue (list(search)[1]['name']=='John Paul')
		print 'PASSED SEARCH' 
	

	def test_elastic_commit(self):
	    #test elastic commit
		docc = {'_id': '3', 'name': 'Waldo', 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		res = ElasticDoc._search()
		assert(len(res) == 0)
		time.sleep(2)
		res = ElasticDoc._search()
		assert(len(res) != 0)
		for it in res:
			assert(it['name'] == 'Waldo')
		print 'PASSED COMMIT'

            
	def test_get_last_doc(self):
		#test get last doc
		docc = {'_id': '4', 'name': 'Hare', '_ts': 3, 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		docc = {'_id': '5', 'name': 'Tortoise', '_ts': 2, 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		docc = {'_id': '6', 'name': 'Mr T.', '_ts': 1, 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		elastic.refresh()
		doc = ElasticDoc.get_last_doc()
		self.assertTrue(doc['_id'] == '4')
		docc = {'_id': '6', 'name': 'HareTwin', '_ts':4, 'ns': 'test.test'}
		ElasticDoc.upsert(docc)
		elastic.refresh()
		doc = ElasticDoc.get_last_doc()
		self.assertTrue(doc['_id'] == '6');
		print 'PASSED GET LAST DOC'


if __name__ == '__main__':
    unittest.main()
