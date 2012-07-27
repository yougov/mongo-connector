"""
Module with code to setup cluster and test oplog_manager functions.

This is the main tester method. All the functions can be called by
finaltests.py
"""

import subprocess
import sys
import time
import os
import json
import unittest
import re

from setup_cluster import killMongoProc, startMongoProc, start_cluster
from optparse import OptionParser
from pymongo import Connection
from pymongo.errors import ConnectionFailure, OperationFailure
from os import path
from threading import Timer
from oplog_manager import OplogThread
from solr_doc_manager import DocManager
from pysolr import Solr
from backend_simulator import BackendSimulator
from util import (long_to_bson_ts,
                  bson_ts_to_long,
                  retry_until_ok)
from checkpoint import Checkpoint
from bson.objectid import ObjectId

""" Global path variables
"""
PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27217"}
PORTS_TWO = {"PRIMARY": "27317", "SECONDARY": "27318", "ARBITER": "27319",
             "CONFIG": "27220", "MONGOS": "27217"}
SETUP_DIR = path.expanduser("~/mongo-connector")
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS_ONE["MONGOS"]
AUTH_KEY = None


def safe_mongo_op(func, arg1, arg2=None):
    """Performs the given operation with the safe argument
    """
    count = 0
    while True:
        time.sleep(1)
        count += 1
        if (count > 60):
            sys.exit(1)
        try:
            if arg2:
                func(arg1, arg2, safe=True)
            else:
                func(arg1, safe=True)
            break
        except OperationFailure:
            pass


class TestOplogManagerSharded(unittest.TestCase):
    """Defines all the testing methods for a sharded cluster
    """

    def runTest(self):
        unittest.TestCase.__init__(self)

    def get_oplog_thread(self):
        """ Set up connection with mongo.

        Returns oplog, the connection and oplog collection.
        This function clears the oplog.
        """
        primary_conn = Connection('localhost', int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection('localhost', int(PORTS_ONE["SECONDARY"]))

        mongos_addr = "localhost:" + PORTS_ONE["MONGOS"]
        mongos = Connection(mongos_addr)
        mongos['alpha']['foo'].drop()

        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog

        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=1000000)
        namespace_set = ['test.test', 'alpha.foo']
        doc_manager = BackendSimulator()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, True,
                            doc_manager, {}, namespace_set, AUTH_KEY)

        return (oplog, primary_conn, oplog_coll, mongos)

    def get_new_oplog(self):
        """ Set up connection with mongo.

        Returns oplog, the connection and oplog collection
        This function does not clear the oplog
        """
        primary_conn = Connection('localhost', int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection('localhost', int(PORTS_ONE["SECONDARY"]))

        mongos = "localhost:" + PORTS_ONE["MONGOS"]
        oplog_coll = primary_conn['local']['oplog.rs']

        namespace_set = ['test.test', 'alpha.foo']
        doc_manager = BackendSimulator()
        oplog = OplogThread(primary_conn, mongos, oplog_coll, True,
                            doc_manager, {}, namespace_set, AUTH_KEY)

        return (oplog, primary_conn, oplog_coll, oplog.mongos_connection)

    def test_retrieve_doc(self):
        """Test retrieve_doc in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, primary_conn, oplog_coll, mongos = self.get_oplog_thread()
        # testing for entry as none type
        entry = None
        assert (test_oplog.retrieve_doc(entry) is None)

        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)

        assert (oplog_cursor.count() == 0)

        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        last_oplog_entry = oplog_cursor.next()
        target_entry = mongos['alpha']['foo'].find_one()

        # testing for search after inserting a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) == target_entry)

        safe_mongo_op(mongos['alpha']['foo'].update, {'name': 'paulie'},
                      {"$set": {'name': 'paul'}})
        last_oplog_entry = oplog_cursor.next()
        target_entry = mongos['alpha']['foo'].find_one()

        # testing for search after updating a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) == target_entry)

        safe_mongo_op(mongos['alpha']['foo'].remove, {'name': 'paul'})
        last_oplog_entry = oplog_cursor.next()

        # testing for search after deleting a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) is None)

        last_oplog_entry['o']['_id'] = 'badID'

        # testing for bad doc id as input
        assert (test_oplog.retrieve_doc(last_oplog_entry) is None)

        # test_oplog.stop()
        print 'PASSED TEST RETRIEVE DOC'

    def test_get_oplog_cursor(self):
        """Test get_oplog_cursor in oplog_manager.

        Assertion failure if it doesn't pass
        """
        test_oplog, primary_conn, oplog_coll, mongos = self.get_oplog_thread()
        # test None cursor
        assert (test_oplog.get_oplog_cursor(None) is None)

        # test with one document
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.get_oplog_cursor(ts)
        assert (cursor.count() == 1)

        # test with two documents, one after the ts
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paul'})
        cursor = test_oplog.get_oplog_cursor(ts)
        assert (cursor.count() == 2)

        print 'PASSED TEST GET OPLOG CURSOR'

    def test_get_last_oplog_timestamp(self):
        """Test get_last_oplog_timestamp in oplog_manager.

        Assertion failure if it doesn't pass
        """

        # test empty oplog
        test_oplog, primary_conn, oplog_coll, mongos = self.get_oplog_thread()

        assert (test_oplog.get_last_oplog_timestamp() is None)

        # test non-empty oplog
        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        last_oplog_entry = oplog_cursor.next()
        last_ts = last_oplog_entry['ts']
        assert (test_oplog.get_last_oplog_timestamp() == last_ts)

        # test_oplog.stop()

        print 'PASSED TEST GET OPLOG TIMESTAMP'

    def test_dump_collection(self):
        """Test dump_collection in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, primary_conn, oplog_coll, mongos = self.get_oplog_thread()
        solr = BackendSimulator()
        test_oplog.doc_manager = solr

        # for empty oplog, no documents added
        assert (test_oplog.dump_collection(None) is None)
        assert (len(solr.test_search()) == 0)

        # with documents
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        test_oplog.dump_collection(search_ts)

        test_oplog.doc_manager.commit()
        solr_results = solr.test_search()
        assert (len(solr_results) == 1)
        solr_doc = solr_results[0]
        assert (long_to_bson_ts(solr_doc['_ts']) == search_ts)
        assert (solr_doc['name'] == 'paulie')
        assert (solr_doc['ns'] == 'alpha.foo')

        print 'PASSED TEST DUMP COLLECTION'

    def test_init_cursor(self):
        """Test init_cursor in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, primary_conn, oplog_coll, mongos = self.get_oplog_thread()
        test_oplog.checkpoint = Checkpoint()         # needed for these tests

        # initial tests with no config file and empty oplog
        assert (test_oplog.init_cursor() is None)

        # no config, single oplog entry
        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.init_cursor()

        assert (cursor.count() == 1)
        assert (test_oplog.checkpoint.commit_ts == search_ts)

        # with config file, assert that size != 0
        os.system('touch temp_config.txt')

        # config_file_path = os.getcwd() + '/'+ 'temp_config.txt'
        # test_oplog.oplog_file = config_file_path
        cursor = test_oplog.init_cursor()
        # config_file_size = os.stat(config_file_path)[6]
        oplog_dict = test_oplog.oplog_progress_dict

        assert(cursor.count() == 1)
        self.assertTrue(str(test_oplog.oplog) in oplog_dict)
        commit_ts = test_oplog.checkpoint.commit_ts
        self.assertTrue(oplog_dict[str(test_oplog.oplog)] == commit_ts)

        os.system('rm temp_config.txt')
        print 'PASSED TEST INIT CURSOR'

    def test_prepare_for_sync(self):
        """Test prepare_for_sync in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, primary_conn, oplog_coll, mongos = self.get_oplog_thread()
        cursor = test_oplog.prepare_for_sync()

        assert (test_oplog.checkpoint is not None)
        assert (test_oplog.checkpoint.commit_ts is None)
        assert (cursor is None)

        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        cursor = test_oplog.prepare_for_sync()
        search_ts = test_oplog.get_last_oplog_timestamp()

        # ensure that the cursor is valid and the timestamp is updated properly
        assert (test_oplog.checkpoint.commit_ts == search_ts)
        assert (cursor is not None)
        assert (cursor.count() == 1)

        safe_mongo_op(mongos['alpha']['foo'].insert, {'name': 'paulter'})
        cursor = test_oplog.prepare_for_sync()
        new_search_ts = test_oplog.get_last_oplog_timestamp()

        # make sure that the newest document is in the cursor.
        assert (cursor.count() == 2)
        next_doc = cursor.next()
        assert (next_doc['o']['name'] == 'paulter')
        assert (next_doc['ts'] == new_search_ts)

        # test_oplog.stop()
        print 'PASSED TEST PREPARE FOR SYNC'

    def test_rollback(self):
        """Test rollback in oplog_manager. Assertion failure if it doesn't pass
            We force a rollback by inserting a doc, killing primary, inserting
            another doc, killing the new primary, and then restarting both
            servers.
        """

        os.system('rm config.txt; touch config.txt')
        start_cluster(sharded=True)

        test_oplog, primary_conn, oplog_coll, mongos = self.get_new_oplog()

        solr = BackendSimulator()
        test_oplog.doc_manager = solr
        solr.test_delete()          # equivalent to solr.delete(q='*:*')
        obj1 = ObjectId('4ff74db3f646462b38000001')

        safe_mongo_op(mongos['alpha']['foo'].remove, {})
        safe_mongo_op(mongos['alpha']['foo'].insert,
                      {'_id': obj1, 'name': 'paulie'})
        cutoff_ts = test_oplog.get_last_oplog_timestamp()

        obj2 = ObjectId('4ff74db3f646462b38000002')
        first_doc = {'name': 'paulie', '_ts': bson_ts_to_long(cutoff_ts),
                     'ns': 'alpha.foo', '_id': obj1}

        # try kill one, try restarting
        killMongoProc(primary_conn.host, PORTS_ONE['PRIMARY'])

        new_primary_conn = Connection('localhost', int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                mongos['alpha']['foo'].insert, {'_id': obj2, 'name': 'paul'}
                break
            except:
                time.sleep(1)
                count += 1
                if count > 60:
                    sys.exit(1)
                continue

        killMongoProc(primary_conn.host, PORTS_ONE['SECONDARY'])

        startMongoProc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)

        # wait for master to be established
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        startMongoProc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
                       "/replset1b.log", None)

        # wait for secondary to be established
        admin_db = new_primary_conn['admin']
        while admin_db.command("replSetGetStatus")['myState'] != 2:
            time.sleep(1)

        while retry_until_ok(mongos['alpha']['foo'].find().count) != 1:
            time.sleep(1)

        self.assertEqual(str(new_primary_conn.port), PORTS_ONE['SECONDARY'])
        self.assertEqual(str(primary_conn.port), PORTS_ONE['PRIMARY'])

        last_ts = test_oplog.get_last_oplog_timestamp()
        second_doc = {'name': 'paul', '_ts': bson_ts_to_long(last_ts),
                      'ns': 'alpha.foo', '_id': obj2}

        test_oplog.doc_manager.upsert(first_doc)
        test_oplog.doc_manager.upsert(second_doc)
        test_oplog.rollback()
        test_oplog.doc_manager.commit()
        results = solr.test_search()

        self.assertEqual(len(results), 1)

        results_doc = results[0]
        self.assertEqual(results_doc['name'], 'paulie')
        self.assertTrue(results_doc['_ts'] <= bson_ts_to_long(cutoff_ts))

        print 'PASSED TEST ROLLBACK'

if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')

    parser = OptionParser()

    # -a is to specify the auth file.
    parser.add_option("-a", "--auth", action="store", type="string",
                      dest="auth_file", default="")

    (options, args) = parser.parse_args()

    if options.auth_file != "":
        start_cluster(sharded=True, key_file=options.auth_file)
        try:
            file = open(options.auth_file)
            key = file.read()
            re.sub(r'\s', '', key)
            AUTH_KEY = key
        except:
           #  logger.error('Could not parse authentication file!')
            exit(1)
    else:
        start_cluster(sharded=True)

    conn = Connection('localhost:' + PORTS_ONE['MONGOS'])
    unittest.main(argv=[sys.argv[0]])
