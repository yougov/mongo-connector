"""Test oplog manager methods
"""

import subprocess
import sys
import time
import os
import json
import re
import unittest

from setup_cluster import killMongoProc, startMongoProc, start_cluster
from pymongo import Connection
from pymongo.errors import ConnectionFailure
from os import path
from optparse import OptionParser
from oplog_manager import OplogThread
from backend_simulator import BackendSimulator
from pysolr import Solr
from util import(long_to_bson_ts,
                 bson_ts_to_long,
                 retry_until_ok)
from checkpoint import Checkpoint
from bson.objectid import ObjectId

""" Global path variables
"""
PORTS_ONE = {"PRIMARY":  "27117", "SECONDARY":  "27118", "ARBITER":  "27119",
             "CONFIG":  "27220", "MONGOS":  "27217"}

AUTH_KEY = None


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """
    def runTest(self):
        unittest.TestCase.__init__(self)

    def get_oplog_thread(self):
        """ Set up connection with mongo. Returns oplog, the connection and
            oplog collection

            This function clears the oplog
        """
        primary_conn = Connection('localhost', int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection('localhost', int(PORTS_ONE["SECONDARY"]))

        primary_conn['test']['test'].drop()
        mongos_addr = "localhost:" + PORTS_ONE["MONGOS"]

        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog

        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=1000000)
        namespace_set = ['test.test']
        doc_manager = BackendSimulator()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, True,
                            doc_manager, {},
                            namespace_set, AUTH_KEY)

        return(oplog, primary_conn, oplog_coll)

    def get_new_oplog(self):
        """ Set up connection with mongo. Returns oplog, the connection and
            oplog collection

            This function does not clear the oplog
        """
        primary_conn = Connection('localhost', int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection('localhost', int(PORTS_ONE["SECONDARY"]))

        mongos = "localhost:" + PORTS_ONE["MONGOS"]
        oplog_coll = primary_conn['local']['oplog.rs']

        namespace_set = ['test.test']
        doc_manager = BackendSimulator()
        oplog = OplogThread(primary_conn, mongos, oplog_coll, True,
                            doc_manager, {},
                            namespace_set, AUTH_KEY)

        return(oplog, primary_conn, oplog.mongos_connection, oplog_coll)

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
        last_oplog_entry = oplog_cursor.next()
        target_entry = primary_conn['test']['test'].find_one()

        #testing for search after inserting a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry),
                         target_entry)

        primary_conn['test']['test'].update({'name': 'paulie'},
                                            {"$set":  {'name': 'paul'}})

        last_oplog_entry = oplog_cursor.next()
        target_entry = primary_conn['test']['test'].find_one()

        #testing for search after updating a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry),
                         target_entry)

        primary_conn['test']['test'].remove({'name': 'paul'})
        last_oplog_entry = oplog_cursor.next()

        #testing for search after deleting a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry), None)

        last_oplog_entry['o']['_id'] = 'badID'

        #testing for bad doc id as input
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry), None)

        #test_oplog.join()
        print 'PASSED TEST RETRIEVE DOC'

    def test_get_oplog_cursor(self):
        """Test get_oplog_cursor in oplog_manager. Assertion failure if
            it doesn't pass
        """
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()

        #test None cursor
        self.assertEqual(test_oplog.get_oplog_cursor(None), None)

        #test with one document
        primary_conn['test']['test'].insert({'name': 'paulie'})
        ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.get_oplog_cursor(ts)
        self.assertEqual(cursor.count(), 1)

        #test with two documents, one after the ts
        primary_conn['test']['test'].insert({'name': 'paul'})
        cursor = test_oplog.get_oplog_cursor(ts)
        self.assertEqual(cursor.count(), 2)

        #test case where timestamp is not in oplog which implies we're too
        #behind
        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog
        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=10000)

        primary_conn['test']['test'].insert({'name': 'pauline'})
        self.assertEqual(test_oplog.get_oplog_cursor(ts), None)
        #test_oplog.join()
        print 'PASSED TEST GET OPLOG CURSOR'
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
        last_oplog_entry = oplog_cursor.next()
        self.assertEqual(test_oplog.get_last_oplog_timestamp(),
                         last_oplog_entry['ts'])

        #test_oplog.join()
        print 'PASSED TEST GET LAST OPLOG TIMESTAMP'

    def test_dump_collection(self):
        """Test dump_collection in oplog_manager. Assertion failure if it
            doesn't pass
        """

        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        solr = BackendSimulator()
        test_oplog.doc_manager = solr

        #for empty oplog, no documents added
        self.assertEqual(test_oplog.dump_collection(None), None)
        self.assertEqual(len(solr.test_search()), 0)

        #with documents
        primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        test_oplog.dump_collection(search_ts)

        test_oplog.doc_manager.commit()
        solr_results = solr.test_search()
        self.assertEqual(len(solr_results), 1)
        solr_doc = solr_results[0]
        self.assertEqual(long_to_bson_ts(solr_doc['_ts']), search_ts)
        self.assertEqual(solr_doc['name'], 'paulie')
        self.assertEqual(solr_doc['ns'], 'test.test')

        #test_oplog.join()
        print 'PASSED TEST DUMP COLLECTION'

    def test_init_cursor(self):
        """Test init_cursor in oplog_manager. Assertion failure if it
            doesn't pass
        """

        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        test_oplog.checkpoint = Checkpoint()    # needed for these tests

        # initial tests with no config file and empty oplog
        self.assertEqual(test_oplog.init_cursor(), None)

        # no config, single oplog entry
        primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.init_cursor()
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(test_oplog.checkpoint.commit_ts, search_ts)

        # with config file, assert that size != 0
        os.system('touch temp_config.txt')

        # config_file_path = os.getcwd() + '/'+ 'temp_config.txt'
        # test_oplog.oplog_file = config_file_path
        cursor = test_oplog.init_cursor()
        # config_file_size = os.stat(config_file_path)[6]
        oplog_dict = test_oplog.oplog_progress_dict

        self.assertEqual(cursor.count(), 1)
        self.assertTrue(str(test_oplog.oplog) in oplog_dict)
        self.assertTrue(oplog_dict[str(test_oplog.oplog)] ==
                        test_oplog.checkpoint.commit_ts)

        os.system('rm temp_config.txt')
        print 'PASSED TEST INIT CURSOR'

    def test_prepare_for_sync(self):
        """Test prepare_for_sync in oplog_manager. Assertion failure
            if it doesn't pass
        """

        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        cursor = test_oplog.prepare_for_sync()

        self.assertTrue(test_oplog.checkpoint is not None)
        self.assertEqual(test_oplog.checkpoint.commit_ts, None)
        self.assertEqual(cursor, None)

        primary_conn['test']['test'].insert({'name': 'paulie'})
        cursor = test_oplog.prepare_for_sync()
        search_ts = test_oplog.get_last_oplog_timestamp()

        # make sure that the cursor is valid and the timestamp is
        # updated properly
        self.assertEqual(test_oplog.checkpoint.commit_ts, search_ts)
        self.assertTrue(cursor is not None)
        self.assertEqual(cursor.count(), 1)

        primary_conn['test']['test'].insert({'name': 'paulter'})
        cursor = test_oplog.prepare_for_sync()
        new_search_ts = test_oplog.get_last_oplog_timestamp()

        #make sure that the newest document is in the cursor.
        self.assertEqual(cursor.count(), 2)
        next_doc = cursor.next()
        self.assertEqual(next_doc['o']['name'], 'paulter')
        self.assertEqual(next_doc['ts'], new_search_ts)

        #test_oplog.join()
        print 'PASSED TEST PREPARE FOR SYNC'

    def test_rollback(self):
        """Test rollback in oplog_manager. Assertion failure if it doesn't pass
        """
        os.system('rm config.txt; touch config.txt')
        start_cluster()
        test_oplog, primary_conn, mongos, oplog_coll = self.get_new_oplog()
        #test_oplog.start()

        solr = BackendSimulator()
        test_oplog.doc_manager = solr
        solr.test_delete()          # equivalent to solr.delete(q='*: *')
        obj1 = ObjectId('4ff74db3f646462b38000001')

        mongos['test']['test'].remove({})
        mongos['test']['test'].insert({'_id': obj1, 'name': 'paulie'},
                                      safe=1)
        cutoff_ts = test_oplog.get_last_oplog_timestamp()

        obj2 = ObjectId('4ff74db3f646462b38000002')
        first_doc = {'name': 'paulie', '_ts': bson_ts_to_long(cutoff_ts),
                     'ns': 'test.test',
                     '_id':  obj1}

        #try kill one, try restarting
        killMongoProc(primary_conn.host, PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))

        admin = new_primary_conn['admin']
        while admin.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        while True:
            try:
                current_conn = mongos['test']['test']
                current_conn.insert({'_id':  obj2, 'name':  'paul'}, safe=1)
                break
            except:
                time.sleep(1)
                continue

        killMongoProc(primary_conn.host, PORTS_ONE['SECONDARY'])
        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)

        #wait for master to be established
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
                time.sleep(1)

        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
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
                      'ns': 'test.test', '_id':  obj2}

        test_oplog.doc_manager.upsert(first_doc)
        test_oplog.doc_manager.upsert(second_doc)

        test_oplog.rollback()
        test_oplog.doc_manager.commit()
        results = solr.test_search()

        assert(len(results) == 1)

        results_doc = results[0]
        self.assertEqual(results_doc['name'], 'paulie')
        self.assertTrue(results_doc['_ts'] <= bson_ts_to_long(cutoff_ts))

        #test_oplog.join()
        print 'PASSED TEST ROLLBACK'

if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')

    parser = OptionParser()

    #-m is for the mongos address, which is a host: port pair.
    parser.add_option("-a", "--auth", action="store", type="string",
                      dest="auth_file", default="")

    (options, args) = parser.parse_args()

    if options.auth_file != "":
        start_cluster(key_file=options.auth_file)
        try:
            file = open(options.auth_file)
            key = file.read()
            re.sub(r'\s', '', key)
            AUTH_KEY = key
        except:
           # logger.error('Could not parse authentication file!')
            exit(1)
    else:
        start_cluster()

    conn = Connection('localhost:' + PORTS_ONE['MONGOS'])
    unittest.main(argv=[sys.argv[0]])
