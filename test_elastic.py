"""Test elastic search using the synchronizer, i.e. as it would be used by an
    user
"""

import time
import unittest
import os
import sys
from setup_cluster import killMongoProc, startMongoProc, start_cluster
from pymongo import Connection
from os import path
from threading import Timer
from doc_manager import DocManager
from pysolr import Solr
from mongo_internal import Connector

""" Global path variables
"""
PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27217"}
s = DocManager('http://localhost:9200', auto_commit=False)
conn = None
NUMBER_OF_DOCS = 100


class TestSynchronizer(unittest.TestCase):

    c = None  # used for the connector

    def runTest(self):
        unittest.TestCase.__init__(self)

    def tearDown(self):
        self.c.doc_manager.auto_commit = False
        time.sleep(2)
        self.c.join()

    def setUp(self):
        self.c = Connector('localhost:' + PORTS_ONE["MONGOS"],
                        'config.txt', 'http://localhost:9200', ['test.test'],
                        '_id', None)
        self.c.start()
        while len(self.c.shard_set) == 0:
            pass
        conn['test']['test'].remove(safe=True)
        while(len(s._search()) != 0):
            time.sleep(1)

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
            properly
        """

        self.assertEqual(len(self.d.shard_set), 1)
        print 'PASSED TEST SHARD LENGTH'

    def test_initial(self):
        """Tests search and assures that the databases are clear.
        """

        conn['test']['test'].remove(safe=True)
        self.assertEqual(conn['test']['test'].find().count(), 0)
        self.assertEqual(len(s._search()), 0)
        print 'PASSED TEST INITIAL'

    def test_insert(self):
        """Tests insert
        """

        conn['test']['test'].insert({'name': 'paulie'}, safe=True)
        while(len(s._search()) == 0):
            time.sleep(1)
        a = s._search()
        self.assertEqual(len(a), 1)
        b = conn['test']['test'].find_one()
        for it in a:
            self.assertEqual(it['_id'], str(b['_id']))
            self.assertEqual(it['name'], b['name'])
        print 'PASSED TEST INSERT'

    def test_remove(self):
        """Tests remove
        """

        conn['test']['test'].insert({'name': 'paulie'}, safe=True)
        while(len(s._search()) != 1):
            time.sleep(1)
        conn['test']['test'].remove({'name': 'paulie'}, safe=True)

        while(len(s._search()) == 1):
            time.sleep(1)
        a = s._search()
        self.assertEqual(len(a), 0)
        print 'PASSED TEST REMOVE'

    def test_rollback(self):
        """Tests rollback
        """

        primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))

        conn['test']['test'].insert({'name': 'paul'}, safe=True)
        while conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)

        killMongoProc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))

        admin = new_primary_conn['admin']
        while admin.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                a = conn['test']['test'].insert({'name': 'pauline'}, safe=True)
                break
            except:
                time.sleep(1)
                count += 1
                if count >= 60:
                    sys.exit(1)
                continue
        while(len(s._search()) != 2):
            time.sleep(1)
        a = s._search()
        b = conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(a), 2)
        for it in a:
            if it['name'] == 'pauline':
                self.assertEqual(it['_id'], str(b['_id']))
        killMongoProc('localhost', PORTS_ONE['SECONDARY'])

        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        time.sleep(2)
        a = s._search()
        self.assertEqual(len(a), 1)
        for it in a:
            self.assertEqual(it['name'], 'paul')
        self.assertEqual(conn['test']['test'].find().count(), 1)
        print 'PASSED TEST ROLLBACK'

    def test_stress(self):
        """Test stress by inserting and removing the number of documents
            specified in global
            variable
        """

        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        while len(s._search()) != NUMBER_OF_DOCS:
            time.sleep(5)
        for i in range(0, NUMBER_OF_DOCS):
            a = s._search()
            b = conn['test']['test'].find_one({'name': 'Paul ' + str(i)})
            for it in a:
                if(it['name'] == 'Paul' + str(i)):
                    self.assertEqual(it['_id'], it['_id'])
        print 'PASSED TEST STRESS'

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
            in global variable.
        """

        while len(s._search()) != 0:
            time.sleep(1)
        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'name': 'Paul ' + str(i)})

        while len(s._search()) != NUMBER_OF_DOCS:
            time.sleep(1)
        primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))
        killMongoProc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))

        admin = new_primary_conn['admin']
        while admin.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        for i in range(0, NUMBER_OF_DOCS):
            try:
                conn['test']['test'].insert({'name': 'Pauline ' + str(i)},
                                            safe=1)
                count += 1
            except:
                time.sleep(1)
                i -= 1
                continue
        while(len(s._search()) != NUMBER_OF_DOCS + count):
            time.sleep(1)
        a = s._search()
        self.assertEqual(len(a), NUMBER_OF_DOCS + count)
        for it in a:
            if 'Pauline' in it['name']:
                b = conn['test']['test'].find_one({'name': it['name']})
                self.assertEqual(it['_id'], str(b['_id']))

        killMongoProc('localhost', PORTS_ONE['SECONDARY'])

        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(1)
        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        while(len(s._search()) != NUMBER_OF_DOCS):
            time.sleep(5)

        a = s._search()
        self.assertEqual(len(a), NUMBER_OF_DOCS)
        for it in a:
            self.assertTrue('Paul' in it['name'])
        self.assertEqual(conn['test']['test'].find().count(), NUMBER_OF_DOCS)

        print 'PASSED TEST STRESSED ROLBACK'


def abort_test(self):
        print 'TEST FAILED'
        sys.exit(1)

if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')
    s._remove()
    start_cluster()
    conn = Connection('localhost:' + PORTS_ONE['MONGOS'])
    unittest.main()
