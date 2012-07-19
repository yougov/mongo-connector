import time
from setup_cluster import killMongoProc, startMongoProc 
from pymongo import Connection
from os import path
from threading import Timer
from solr_doc_manager import DocManager
from pysolr import Solr
from mongo_internal import Daemon


""" Global path variables
"""    
PORTS_ONE = {"PRIMARY":"27117", "SECONDARY":"27118", "ARBITER":"27119", 
    "CONFIG":"27220", "MONGOS":"27217"}


class TestSynchronizer():

	def __init__(self):
		self.conn = Connection('localhost:' + PORTS_ONE['MONGOS'])
		
		while (True):
			try:
				self.conn['test']['test'].remove(safe = True)
				break
			except:
				continue

		#establish solr and mongo

		self.s = Solr('http://localhost:8080/solr')
		self.s.delete(q = '*:*')
		
		t = Timer(60, self.abort_test)
		t.start()
		self.d = Daemon('localhost:' + PORTS_ONE["MONGOS"], 'config.txt', 'http://localhost:8080/solr', ['test.test'], '_id')
		self.d.start()
		while len(self.d.shard_set) == 0:
			pass
		t.cancel()
		#the Daemon should recognize a single running shard
		assert len(self.d.shard_set) == 1
		
	def abort_test(self):
		print 'test failed'
		sys.exit(1)
	
	def initial_test (self):

		#test search + initial clear
		assert (self.conn['test']['test'].find().count() == 0)
		assert (len(self.s.search('*:*')) == 0)
		
	def insert_test(self):
		#test insert
		self.conn['test']['test'].insert ( {'name':'paulie'}, safe=True )
		while (len(self.s.search('*:*')) == 0):
			time.sleep(1)
		a = self.s.search('paulie')
		assert (len(a) == 1)
		b = self.conn['test']['test'].find_one()
		for it in a:
			assert (it['_id'] == str(b['_id']))
			assert (it['name'] == b['name'])


	def remove_test (self):
		#test remove
		self.conn['test']['test'].remove({'name':'paulie'}, safe=True)
		while (len(self.s.search('*:*')) == 1):
			time.sleep(1)
		a = self.s.search('paulie')
		assert (len(a) == 0)
	
	def rollback_test(self):
		#test rollback
		primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))

		self.conn['test']['test'].insert({'name': 'paul'}, safe=True)
		while self.conn['test']['test'].find({'name': 'paul'}).count() != 1:
			time.sleep(1)
				 
		killMongoProc('localhost', PORTS_ONE['PRIMARY'])

		new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
	
		while new_primary_conn['admin'].command("isMaster")['ismaster'] is False:
			time.sleep(1)
		time.sleep(5)
		while True:
			try:
				a = self.conn['test']['test'].insert({'name': 'pauline'}, safe=True)
				break
			except: 
				time.sleep(1)
				continue
		while (len(self.s.search('*:*')) != 2):
			time.sleep(1)
		a = self.s.search('pauline')
		b = self.conn['test']['test'].find_one({'name':'pauline'})
		assert (len(a) == 1)
		for it in a:
			assert (it['_id'] == str(b['_id']))
		killMongoProc('localhost', PORTS_ONE['SECONDARY'])

		startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a", "/replset1a.log")
		while primary_conn['admin'].command("isMaster")['ismaster'] is False:
			time.sleep(1)
		
		startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b", "/replset1b.log")

		time.sleep(2)
		a = self.s.search('pauline')
		assert (len(a) == 0)
		a = self.s.search('paul')
		assert (len(a) == 1)
		self.conn['test']['test'].remove()
		
	def stress_test(self):
		#stress test
		self.NUMBER_OF_DOCS = 100
		for i in range(0, self.NUMBER_OF_DOCS):
			self.conn['test']['test'].insert({'name': 'Paul '+str(i)})
		time.sleep(5)
		while len(self.s.search('*:*', rows=self.NUMBER_OF_DOCS)) != self.NUMBER_OF_DOCS:
			time.sleep(5)
	   # self.conn['test']['test'].create_index('name')
		for i in range(0, self.NUMBER_OF_DOCS):
			a = self.s.search('Paul ' + str(i))
			b = self.conn['test']['test'].find_one({'name': 'Paul ' + str(i)})
			for it in a:
				assert (it['_id'] == it['_id']) 

				   
	
	def stressed_rollback_test(self):
		#test stressed rollback         
		primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))
		killMongoProc('localhost', PORTS_ONE['PRIMARY'])
		 
		new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
		
		while new_primary_conn['admin'].command("isMaster")['ismaster'] is False:
			time.sleep(1)
		time.sleep(5)
		for i in range(0, self.NUMBER_OF_DOCS):
			try:
				self.conn['test']['test'].insert({'name': 'Pauline ' + str(i)}, safe=True)
			except: 
				time.sleep(1)
				i -= 1
				continue
		while (len(self.s.search('*:*', rows = self.NUMBER_OF_DOCS*2)) != self.NUMBER_OF_DOCS*2):
			time.sleep(1)
		a = self.s.search('Pauline', rows = self.NUMBER_OF_DOCS*2, sort='_id asc')
		assert (len(a) == self.NUMBER_OF_DOCS)
		i = 0
		for it in a:
			b = self.conn['test']['test'].find_one({'name': 'Pauline ' + str(i)})
			i += 1
			assert (it['_id'] == str(b['_id']))
	
		killMongoProc('localhost', PORTS_ONE['SECONDARY'])
		 
		startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a", "/replset1a.log")
		while primary_conn['admin'].command("isMaster")['ismaster'] is False:
			time.sleep(1)
			
		startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b", "/replset1b.log")
		
		while (len( self.s.search('Pauline', rows = self.NUMBER_OF_DOCS*2)) != 0):
			time.sleep(15)
		a = self.s.search('Pauline', rows = self.NUMBER_OF_DOCS*2)
		assert (len(a) == 0)
		a = self.s.search('Paul', rows = self.NUMBER_OF_DOCS*2)
		assert (len(a) == self.NUMBER_OF_DOCS)
				 
		self.d.stop()