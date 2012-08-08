
# Copyright 2012 10gen, Inc.
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

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Test synchronizer using DocManagerSimulator
"""
import os
import sys
import inspect

file = inspect.getfile(inspect.currentframe())
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))

doc_folder = cmd_folder.rsplit("/", 1)[0]
doc_folder += '/doc_managers'
if doc_folder not in sys.path:
    sys.path.insert(0, doc_folder)

cmd_folder = cmd_folder.rsplit("/", 1)[0]
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


import time
import unittest
from setup_cluster import killMongoProc, startMongoProc, start_cluster
from pymongo import Connection
from os import path
from threading import Timer
from doc_manager_simulator import DocManager
from pysolr import Solr
from mongo_connector import Connector
from optparse import OptionParser
from util import retry_until_ok
from pymongo.errors import ConnectionFailure, OperationFailure, AutoReconnect

""" Global path variables
"""
PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27117"}
conn = None
NUMBER_OF_DOCS = 100
c = None
s = None


class TestSynchronizer(unittest.TestCase):

    def runTest(self):
        unittest.TestCase.__init__(self)

    def setUp(self):
        conn['test']['test'].remove(safe=True)
        while (len(s._search()) != 0):
            time.sleep(1)

    def test_shard_length(self):
        """Tests the shard_length to see if the shard set was recognized
        """
        self.assertEqual(len(c.shard_set), 1)
        print("PASSED TEST SHARD LENGTH")

    def test_initial(self):
        """Tests search and assures that the databases are clear.
        """
        conn['test']['test'].remove(safe=True)
        s._delete()
        self.assertEqual(conn['test']['test'].find().count(), 0)
        self.assertEqual(len(s._search()), 0)
        print("PASSED TEST INITIAL")

    def test_insert(self):
        """Tests insert
        """
        conn['test']['test'].insert({'name': 'paulie'}, safe=True)
        while (len(s._search()) == 0):
            time.sleep(1)
        a = s._search()
        self.assertEqual(len(a), 1)
        b = conn['test']['test'].find_one()
        for it in a:
            self.assertEqual(it['_id'], b['_id'])
            self.assertEqual(it['name'], b['name'])
        print("PASSED TEST INSERT")

    def test_remove(self):
        """Tests remove
        """

        conn['test']['test'].insert({'name': 'paulie'}, safe=True)
        while (len(s._search()) != 1):
            time.sleep(1)
        conn['test']['test'].remove({'name': 'paulie'}, safe=True)

        while (len(s._search()) == 1):
            time.sleep(1)
        a = s._search()
        self.assertEqual(len(a), 0)
        print("PASSED TEST REMOVE")

    def test_rollback(self):
        """Tests rollback. Rollback is performed by inserting one document,
            killing primary, inserting another doc, killing secondary,
            and then restarting both.
        """
        primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))

        conn['test']['test'].insert({'name': 'paul'}, safe=True)
        while conn['test']['test'].find({'name': 'paul'}).count() != 1:
            time.sleep(1)

        killMongoProc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']

        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                a = conn['test']['test'].insert({'name': 'pauline'}, safe=True)
                break
            except:
                count += 1
                if count > 60:
                    string = 'Call to insert failed too many times'
                    string += ' in test_rollback'
                    logging.error(string)
                    sys.exit(1)
                time.sleep(1)
                continue
        while (len(s._search()) != 2):
            time.sleep(1)
        a = s._search()
        b = conn['test']['test'].find_one({'name': 'pauline'})
        self.assertEqual(len(a), 2)
        for it in a:
            if it['name'] == 'pauline':
                self.assertEqual(it['_id'], b['_id'])
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
        find_cursor = retry_until_ok(conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), 1)
                #self.assertEqual(conn['test']['test'].find().count(), 1)
        print("PASSED TEST ROLLBACK")

    def test_stress(self):
        """Stress test the system by inserting a large number of docs.
        """
        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'name': 'Paul ' + str(i)})
        time.sleep(5)
        while len(s._search()) != NUMBER_OF_DOCS:
            time.sleep(5)
       # conn['test']['test'].create_index('name')
        for i in range(0, NUMBER_OF_DOCS):
            #a = s.search('Paul ' + str(i))
            a = s._search()
            b = conn['test']['test'].find_one({'name': 'Paul ' + str(i)})
            for it in a:
                if (it['name'] == 'Paul' + str(i)):
                    self.assertEqual(it['_id'], it['_id'])
        print("PASSED TEST STRESS")

    def test_stressed_rollback(self):
        """Test stressed rollback with number of documents equal to specified
        in global variable. Rollback is performed like before, but with more
            documents.
        """
        while len(s._search()) != 0:
            time.sleep(1)
        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'name': 'Paul ' + str(i)}, safe=True)

        while len(s._search()) != NUMBER_OF_DOCS:
            time.sleep(1)
        primary_conn = Connection('localhost', int(PORTS_ONE['PRIMARY']))
        killMongoProc('localhost', PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = -1
        while count + 1 < NUMBER_OF_DOCS:
            try:
                count += 1
                conn['test']['test'].insert({'name': 'Pauline ' + str(count)},
                                            safe=True)
            except (OperationFailure, AutoReconnect):
                time.sleep(1)
        while (len(s._search()) != conn['test']['test'].find().count()):
            time.sleep(1)
        a = s._search()
        i = 0
        for it in a:
            if 'Pauline' in it['name']:
                b = conn['test']['test'].find_one({'name': it['name']})
                self.assertEqual(it['_id'], b['_id'])

        killMongoProc('localhost', PORTS_ONE['SECONDARY'])

        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        while (len(s._search()) != NUMBER_OF_DOCS):
            time.sleep(5)

        a = s._search()
        self.assertEqual(len(a), NUMBER_OF_DOCS)
        for it in a:
            self.assertTrue('Paul' in it['name'])
        find_cursor = retry_until_ok(conn['test']['test'].find)
        self.assertEqual(retry_until_ok(find_cursor.count), NUMBER_OF_DOCS)
        print("PASSED TEST STRESSED ROLBACK")


def abort_test(self):
        print("TEST FAILED")
        sys.exit(1)


if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')
    parser = OptionParser()

    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    parser.add_option("-m", "--main", action="store", type="string",
                      dest="main_addr", default="27217")

    (options, args) = parser.parse_args()
    PORTS_ONE['MONGOS'] = options.main_addr
    c = Connector('localhost:' + PORTS_ONE["MONGOS"], 'config.txt', None,
                  ['test.test'], '_id', None, None)
    s = c.doc_manager
    if options.main_addr != "27217":
        start_cluster(use_mongos=False)
    else:
        start_cluster()
    conn = Connection('localhost:' + PORTS_ONE['MONGOS'],
                      replicaSet="demo-repl")
    t = Timer(60, abort_test)
    t.start()
    c.start()
    while len(c.shard_set) == 0:
        pass
    t.cancel()
    unittest.main(argv=[sys.argv[0]])
    c.join()
