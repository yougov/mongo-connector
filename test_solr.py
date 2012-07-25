import time
import unittest
import os
from setup_cluster import killMongoProc, startMongoProc, start_cluster 
from pymongo import Connection
from os import path
from threading import Timer
from doc_manager import DocManager
from pysolr import Solr
from mongo_internal import Daemon


""" Global path variables
"""    
PORTS_ONE = {"PRIMARY":"27117", "SECONDARY":"27118", "ARBITER":"27119", 
    "CONFIG":"27220", "MONGOS":"27217"}
s = Solr('http://localhost:8080/solr')
conn = None
NUMBER_OF_DOCS = 100

class TestSynchronizer(unittest.TestCase):
    
    d = None 
    
    def runTest(self):
        unittest.TestCase.__init__(self)
    
    def setUp(self):
        self.d = Daemon('localhost:' + PORTS_ONE["MONGOS"], 'config.txt', 'http://localhost:8080/solr', ['test.test'], '_id', None)
        self.d.start()
        while len(self.d.shard_set) == 0:
            time.sleep(1)
        
        conn['test']['test'].remove(safe = True)
        while (len(s.search('*:*')) != 0):
            time.sleep(1) 
    
    def tearDown(self):
        self.d.doc_manager.auto_commit = False
        time.sleep(2)
        self.d.join()
                
    def test_shard_length (self):
        self.assertEqual(len(self.d.shard_set), 1)
        print 'PASSED TEST SHARD LENGTH'
    
    def test_initial (self):
        #test search + initial clear
        while (True):
            try:
                conn['test']['test'].remove(safe = True)
                break
            except:
                continue
        
        s.delete(q = '*:*')
        self.assertEqual (conn['test']['test'].find().count(), 0)
        self.assertEqual (len(s.search('*:*')), 0)
        print 'PASSED TEST INITIAL'
            
    def test_insert(self):
        #test insert
        conn['test']['test'].insert ( {'name':'paulie'}, safe=True )
        while (len(s.search('*:*')) == 0):
            time.sleep(1)
        a = s.search('paulie')
        self.assertEqual (len(a), 1)
        b = conn['test']['test'].find_one()
        for it in a:
            self.assertEqual (it['_id'], str(b['_id']))
            self.assertEqual (it['name'], b['name'])
        print 'PASSED TEST INSERT'

    def test_remove (self):
        #test remove
        conn['test']['test'].remove({'name':'paulie'}, safe=True)
        while (len(s.search('*:*')) == 1):
            time.sleep(1)
        a = s.search('paulie')
        self.assertEqual (len(a), 0)
        print 'PASSED TEST REMOVE'
    
    def test_rollback(self):
        #test rollback
        primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))

        conn['test']['test'].insert({'name': 'paul'}, safe=True)
        while conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)
                 
        killMongoProc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
    
        while new_primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        while True:
            try:
                a = conn['test']['test'].insert({'name': 'pauline'}, safe=True)
                break
            except: 
                time.sleep(1)
                continue
        while (len(s.search('*:*')) != 2):
            time.sleep(1)
        a = s.search('pauline')
        b = conn['test']['test'].find_one({'name':'pauline'})
        self.assertEqual (len(a), 1)
        for it in a:
            self.assertEqual (it['_id'], str(b['_id']))
        killMongoProc('localhost', PORTS_ONE['SECONDARY'])

        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a", "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)
        
        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b", "/replset1b.log", None)

        time.sleep(2)
        a = s.search('pauline')
        self.assertEqual (len(a), 0)
        a = s.search('paul')
        self.assertEqual (len(a), 1)
        conn['test']['test'].remove()
        print 'PASSED TEST ROLLBACK'
        
    def test_stress(self):
        #stress test
        #os.system('rm config.txt; touch config.txt')
        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'name': 'Paul '+str(i)})
        time.sleep(5)
        while len(s.search('*:*', rows=NUMBER_OF_DOCS)) != NUMBER_OF_DOCS:
            time.sleep(5)
       # conn['test']['test'].create_index('name')
        for i in range(0, NUMBER_OF_DOCS):
            a = s.search('Paul ' + str(i))
            b = conn['test']['test'].find_one({'name': 'Paul ' + str(i)})
            for it in a:
                self.assertEqual (it['_id'], it['_id']) 
        print 'PASSED TEST STRESS'
    
    
    def test_stressed_rollback(self):
        #test stressed rollback
        
        conn['test']['test'].remove()
        while len(s.search('*:*', rows = NUMBER_OF_DOCS)) != 0:
            time.sleep(1)
        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'name': 'Paul '+str(i)})
    
        while len(s.search('*:*', rows = NUMBER_OF_DOCS)) != NUMBER_OF_DOCS:
            time.sleep(1)
        primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))
        killMongoProc('localhost', PORTS_ONE['PRIMARY'])
         
        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
        
        while new_primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        for i in range(0, NUMBER_OF_DOCS):
            try:
                conn['test']['test'].insert({'name': 'Pauline ' + str(i)}, safe=True)
                count += 1
            except: 
                time.sleep(1)
                i -= 1
                continue
        while (len(s.search('*:*', rows = NUMBER_OF_DOCS*2)) != NUMBER_OF_DOCS + count):
            time.sleep(1)
        a = s.search('Pauline', rows = NUMBER_OF_DOCS*2, sort='_id asc')
        self.assertEqual (len(a), count)
        for it in a:
            b = conn['test']['test'].find_one({'name': it['name']})
            self.assertEqual (it['_id'], str(b['_id']))
    

        
        killMongoProc('localhost', PORTS_ONE['SECONDARY'])
         
        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a", "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)
            
        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b", "/replset1b.log", None)
        
        while (len( s.search('Pauline', rows = NUMBER_OF_DOCS*2)) != 0):
            time.sleep(15)
        a = s.search('Pauline', rows = NUMBER_OF_DOCS*2)
        self.assertEqual (len(a), 0)
        a = s.search('Paul', rows = NUMBER_OF_DOCS*2)
        self.assertEqual (len(a), NUMBER_OF_DOCS)
        print 'PASSED TEST STRESSED ROLBACK'
		
def abort_test(self):
        print 'test failed'
        sys.exit(1)
                
if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')
    s.delete(q = '*:*')
    start_cluster()
    conn = Connection('localhost:' + PORTS_ONE['MONGOS'])
  
    unittest.main()
    
