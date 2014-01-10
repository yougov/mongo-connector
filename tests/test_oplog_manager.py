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

"""Test oplog manager methods
"""

import os
import sys
import inspect
import time
import unittest
import re
import socket
try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection    

sys.path[0:0] = [""]

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from tests.setup_cluster import (kill_mongo_proc,
                                          start_mongo_proc,
                                          start_cluster,
                                          kill_all)
from pymongo.errors import OperationFailure
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.util import(long_to_bson_ts,
                                 bson_ts_to_long,
                                 retry_until_ok)
from bson.objectid import ObjectId

PORTS_ONE = {"PRIMARY":  "27117", "SECONDARY":  "27118", "ARBITER":  "27119",
             "CONFIG":  "27220", "MAIN":  "27217"}

AUTH_FILE = os.environ.get('AUTH_FILE', None)
AUTH_USERNAME = os.environ.get('AUTH_USERNAME', None)
HOSTNAME = os.environ.get('HOSTNAME', socket.gethostname())
PORTS_ONE['MAIN'] = os.environ.get('MAIN_ADDR', "27217")
CONFIG = os.environ.get('CONFIG', "config.txt")
TEMP_CONFIG = os.environ.get('TEMP_CONFIG', "temp_config.txt")

class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """
    def runTest(self):
        """Runs all Tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):
        """ Initializes the cluster
        """
        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        cls.AUTH_KEY = None
        cls.flag = True
        if AUTH_FILE:
            # We want to get the credentials from this file
            try:
                key = (open(AUTH_FILE)).read()
                re.sub(r'\s', '', key)
                cls.AUTH_KEY = key
            except IOError:
                print('Could not parse authentication file!')
                cls.flag = False
                cls.err_msg = "Could not read key file!"

        if not start_cluster(key_file=AUTH_FILE):
            cls.flag = False
            cls.err_msg = "Shards cannot be added to mongos"

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def setUp(self):
        """ Fails if we were unable to start the cluster
        """
        if not self.flag:
            self.fail(self.err_msg)

    @classmethod
    def get_oplog_thread(cls):
        """ Set up connection with mongo. Returns oplog, the connection and
            oplog collection

            This function clears the oplog
        """
        is_sharded = True
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        primary_conn['test']['test'].drop()
        mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])

        if PORTS_ONE["MAIN"] == PORTS_ONE["PRIMARY"]:
            mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])
            is_sharded = False
        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog

        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=1000000)
        namespace_set = ['test.test']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, is_sharded,
                            doc_manager, LockingDict(),
                            namespace_set, cls.AUTH_KEY, AUTH_USERNAME,
                            repl_set="demo-repl")

        return(oplog, primary_conn, oplog_coll)
    
    @classmethod    
    def get_new_oplog(cls):
        """ Set up connection with mongo. Returns oplog, the connection and
            oplog collection

            This function does not clear the oplog
        """
        is_sharded = True
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])
        if PORTS_ONE["MAIN"] == PORTS_ONE["PRIMARY"]:
            mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])
            is_sharded = False
        oplog_coll = primary_conn['local']['oplog.rs']

        namespace_set = ['test.test']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, is_sharded,
                            doc_manager, LockingDict(),
                            namespace_set, cls.AUTH_KEY, AUTH_USERNAME,
                            repl_set="demo-repl")
        return(oplog, primary_conn, oplog.main_connection, oplog_coll)

    def test_retrieve_doc(self):
        """Test retrieve_doc in oplog_manager. Assertion failure if it doesn't
            pass
        """
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        #testing for entry as none type
        entry = None
        self.assertTrue(test_oplog.retrieve_doc(entry) is None)

        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)

        primary_conn['test']['test'].insert({'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        target_entry = primary_conn['test']['test'].find_one()

        #testing for search after inserting a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry),
                         target_entry)

        primary_conn['test']['test'].update({'name': 'paulie'},
                                            {"$set":  {'name': 'paul'}})

        last_oplog_entry = next(oplog_cursor)
        target_entry = primary_conn['test']['test'].find_one()

        #testing for search after updating a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry),
                         target_entry)

        primary_conn['test']['test'].remove({'name': 'paul'})
        last_oplog_entry = next(oplog_cursor)

        #testing for search after deleting a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry), None)

        last_oplog_entry['o']['_id'] = 'badID'

        #testing for bad doc id as input
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry), None)

        #test_oplog.join()

    def test_get_oplog_cursor(self):
        """Test get_oplog_cursor in oplog_manager. Assertion failure if
            it doesn't pass
        """
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()

        #test None cursor
        self.assertEqual(test_oplog.get_oplog_cursor(None), None)

        #test with one document
        primary_conn['test']['test'].insert({'name': 'paulie'})
        timestamp = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.get_oplog_cursor(timestamp)
        self.assertEqual(cursor.count(), 1)

        #test with two documents, one after the timestamp
        primary_conn['test']['test'].insert({'name': 'paul'})
        cursor = test_oplog.get_oplog_cursor(timestamp)
        self.assertEqual(cursor.count(), 2)

        #test case where timestamp is not in oplog which implies we're too
        #behind
        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog
        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=10000)

        primary_conn['test']['test'].insert({'name': 'pauline'})
        self.assertEqual(test_oplog.get_oplog_cursor(timestamp), None)
        #test_oplog.join()
        #need to add tests for 'except' part of get_oplog_cursor

    def test_get_last_oplog_timestamp(self):
        """Test get_last_oplog_timestamp in oplog_manager. Assertion failure
            if it doesn't pass
        """

        #test empty oplog
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        self.assertEqual(test_oplog.get_last_oplog_timestamp(), None)

        #test non-empty oplog
        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)
        primary_conn['test']['test'].insert({'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        self.assertEqual(test_oplog.get_last_oplog_timestamp(),
                         last_oplog_entry['ts'])

        #test_oplog.join()

    def test_dump_collection(self):
        """Test dump_collection in oplog_manager. Assertion failure if it
            doesn't pass
        """

        test_oplog, primary_conn, search_ts = self.get_oplog_thread()
        solr = DocManager()
        test_oplog.doc_manager = solr

        #with documents
        primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        test_oplog.dump_collection()

        test_oplog.doc_manager.commit()
        solr_results = solr._search()
        self.assertEqual(len(solr_results), 1)
        solr_doc = solr_results[0]
        self.assertEqual(long_to_bson_ts(solr_doc['_ts']), search_ts)
        self.assertEqual(solr_doc['name'], 'paulie')
        self.assertEqual(solr_doc['ns'], 'test.test')

    def test_init_cursor(self):
        """Test init_cursor in oplog_manager. Assertion failure if it
            doesn't pass
        """

        test_oplog, primary_conn, search_ts = self.get_oplog_thread()
        test_oplog.checkpoint = None  # needed for these tests

        # initial tests with no config file and empty oplog
        self.assertEqual(test_oplog.init_cursor(), None)

        # no config, single oplog entry
        primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.init_cursor()
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(test_oplog.checkpoint, search_ts)

        # with config file, assert that size != 0
        os.system('touch temp_config.txt')

        cursor = test_oplog.init_cursor()
        oplog_dict = test_oplog.oplog_progress.get_dict()

        self.assertEqual(cursor.count(), 1)
        self.assertTrue(str(test_oplog.oplog) in oplog_dict)
        self.assertTrue(oplog_dict[str(test_oplog.oplog)] ==
                        test_oplog.checkpoint)

        os.system('rm temp_config.txt')

        # test init_cursor when OplogThread created with/without no-dump option
        # insert some documents (will need to be dumped)
        primary_conn['test']['test'].remove()
        primary_conn['test']['test'].insert(({"_id":i} for i in range(100)))

        # test no-dump option
        docman = DocManager()
        docman._delete()
        test_oplog.doc_manager = docman
        test_oplog.collection_dump = False
        test_oplog.oplog_progress = LockingDict()
        # init_cursor has the side-effect of causing a collection dump
        test_oplog.init_cursor()
        self.assertEqual(len(docman._search()), 0)

        # test w/o no-dump option
        docman._delete()
        test_oplog.collection_dump = True
        test_oplog.oplog_progress = LockingDict()
        test_oplog.init_cursor()
        self.assertEqual(len(docman._search()), 100)

    def test_rollback(self):
        """Test rollback in oplog_manager. Assertion failure if it doesn't pass
            We force a rollback by inserting a doc, killing the primary,
            inserting another doc, killing the new primary, and then restarting
            both.
        """
        os.system('rm config.txt; touch config.txt')
        test_oplog, primary_conn, mongos, solr = self.get_new_oplog()

        if not start_cluster():
            self.fail('Cluster could not be started successfully!')

        solr = DocManager()
        test_oplog.doc_manager = solr
        solr._delete()          # equivalent to solr.delete(q='*: *')

        mongos['test']['test'].remove({})
        mongos['test']['test'].insert( 
             {'_id': ObjectId('4ff74db3f646462b38000001'),
             'name': 'paulie'},
             safe=True
             )
        while (mongos['test']['test'].find().count() != 1):
            time.sleep(1)
        cutoff_ts = test_oplog.get_last_oplog_timestamp()

        first_doc = {'name': 'paulie', '_ts': bson_ts_to_long(cutoff_ts),
                     'ns': 'test.test',
                     '_id':  ObjectId('4ff74db3f646462b38000001')}

        #try kill one, try restarting
        kill_mongo_proc(primary_conn.host, PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin = new_primary_conn['admin']
        while admin.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                mongos['test']['test'].insert({
                    '_id': ObjectId('4ff74db3f646462b38000002'),
                    'name': 'paul'}, 
                    safe=True)
                break
            except OperationFailure:
                count += 1
                if count > 60:
                    self.fail('Call to insert doc failed too many times')
                time.sleep(1)
                continue
        while (mongos['test']['test'].find().count() != 2):
            time.sleep(1)
        kill_mongo_proc(primary_conn.host, PORTS_ONE['SECONDARY'])
        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)

        #wait for master to be established
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        #wait for secondary to be established
        admin = new_primary_conn['admin']
        while admin.command("replSetGetStatus")['myState'] != 2:
            time.sleep(1)
        while retry_until_ok(mongos['test']['test'].find().count) != 1:
            time.sleep(1)

        self.assertEqual(str(new_primary_conn.port), PORTS_ONE['SECONDARY'])
        self.assertEqual(str(primary_conn.port), PORTS_ONE['PRIMARY'])

        last_ts = test_oplog.get_last_oplog_timestamp()
        second_doc = {'name': 'paul', '_ts': bson_ts_to_long(last_ts),
                      'ns': 'test.test', 
                      '_id': ObjectId('4ff74db3f646462b38000002')}

        test_oplog.doc_manager.upsert(first_doc)
        test_oplog.doc_manager.upsert(second_doc)

        test_oplog.rollback()
        test_oplog.doc_manager.commit()
        results = solr._search()

        assert(len(results) == 1)

        self.assertEqual(results[0]['name'], 'paulie')
        self.assertTrue(results[0]['_ts'] <= bson_ts_to_long(cutoff_ts))

        #test_oplog.join()

if __name__ == '__main__':
    unittest.main()
