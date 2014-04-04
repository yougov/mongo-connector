# Copyright 20134-2014 MongoDB, Inc.
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

"""
Module with code to setup cluster and test oplog_manager functions.

This is the main tester method. All the functions can be called by
finaltests.py
"""
import os
import sys
import inspect
import socket

sys.path[0:0] = [""]

import time
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
import re

from pymongo import MongoClient

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from tests.setup_cluster import (kill_mongo_proc,
                                 start_mongo_proc,
                                 start_cluster,
                                 kill_all)
from pymongo.errors import OperationFailure
from os import path
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.util import (long_to_bson_ts,
                                  bson_ts_to_long,
                                  retry_until_ok)
from bson.objectid import ObjectId

PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27217"}
PORTS_TWO = {"PRIMARY": "27317", "SECONDARY": "27318", "ARBITER": "27319",
             "CONFIG": "27220", "MONGOS": "27217"}
SETUP_DIR = path.expanduser("~/mongo_connector")
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS_ONE["MONGOS"]
AUTH_FILE = os.environ.get('AUTH_FILE', None)
AUTH_USERNAME = os.environ.get('AUTH_USERNAME', None)
HOSTNAME = os.environ.get('HOSTNAME', socket.gethostname())
TEMP_CONFIG = os.environ.get('TEMP_CONFIG', "temp_config.txt")
CONFIG = os.environ.get('CONFIG', "config.txt")


class TestOplogManagerSharded(unittest.TestCase):
    """Defines all the testing methods for a sharded cluster
    """

    def runTest(self):
        """ Runs the tests
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

        if not start_cluster(sharded=True, key_file=cls.AUTH_KEY):
            cls.flag = False
            cls.err_msg = "Shards cannot be added to mongos"
    
    def setUp(self):
        """ Fails if we can't read the key file or if the
        cluster cannot be created.
        """
        if not self.flag:
            self.fail(self.err_msg)
    
    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    @classmethod
    def get_oplog_thread(cls):
        """ Set up connection with mongo.

        Returns oplog, the connection and oplog collection.
        This function clears the oplog.
        """
        primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE["PRIMARY"]),
                                   replicaSet="demo-repl")

        mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE["MONGOS"])
        mongos = MongoClient(mongos_addr)
        mongos['alpha']['foo'].drop()

        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog

        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=1000000)
        namespace_set = ['test.test', 'alpha.foo']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, True,
                            doc_manager, LockingDict(), namespace_set,
                            cls.AUTH_KEY, AUTH_USERNAME)

        return (oplog, primary_conn, oplog_coll, mongos)

    @classmethod
    def get_new_oplog(cls):
        """ Set up connection with mongo.

        Returns oplog, the connection and oplog collection
        This function does not clear the oplog
        """
        primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        mongos = "%s:%s" % (HOSTNAME, PORTS_ONE["MONGOS"])
        oplog_coll = primary_conn['local']['oplog.rs']

        namespace_set = ['test.test', 'alpha.foo']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos, oplog_coll, True,
                            doc_manager, LockingDict(), namespace_set,
                            cls.AUTH_KEY, AUTH_USERNAME)

        return (oplog, primary_conn, oplog_coll, oplog.main_connection)

    def test_retrieve_doc(self):
        """Test retrieve_doc in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, oplog_cursor, oplog_coll, mongos = self.get_oplog_thread()
        # testing for entry as none type
        entry = None
        assert (test_oplog.retrieve_doc(entry) is None)

        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)

        assert (oplog_cursor.count() == 0)

        retry_until_ok(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        target_entry = mongos['alpha']['foo'].find_one()

        # testing for search after inserting a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) == target_entry)

        retry_until_ok(mongos['alpha']['foo'].update, {'name': 'paulie'},
                       {"$set": {'name': 'paul'}})
        last_oplog_entry = next(oplog_cursor)
        target_entry = mongos['alpha']['foo'].find_one()

        # testing for search after updating a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) == target_entry)

        retry_until_ok(mongos['alpha']['foo'].remove, {'name': 'paul'})
        last_oplog_entry = next(oplog_cursor)

        # testing for search after deleting a document
        assert (test_oplog.retrieve_doc(last_oplog_entry) is None)

        last_oplog_entry['o']['_id'] = 'badID'

        # testing for bad doc id as input
        assert (test_oplog.retrieve_doc(last_oplog_entry) is None)

        # test_oplog.stop()

    def test_get_oplog_cursor(self):
        """Test get_oplog_cursor in oplog_manager.

        Assertion failure if it doesn't pass
        """
        test_oplog, timestamp, cursor, mongos = self.get_oplog_thread()
        # test None cursor
        assert (test_oplog.get_oplog_cursor(None) is None)

        # test with one document
        retry_until_ok(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        timestamp = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.get_oplog_cursor(timestamp)
        assert (cursor.count() == 1)

        # test with two documents, one after the ts
        retry_until_ok(mongos['alpha']['foo'].insert, {'name': 'paul'})
        cursor = test_oplog.get_oplog_cursor(timestamp)
        assert (cursor.count() == 2)


    def test_get_last_oplog_timestamp(self):
        """Test get_last_oplog_timestamp in oplog_manager.

        Assertion failure if it doesn't pass
        """

        # test empty oplog
        test_oplog, oplog_cursor, oplog_coll, mongos = self.get_oplog_thread()

        assert (test_oplog.get_last_oplog_timestamp() is None)

        # test non-empty oplog
        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)
        retry_until_ok(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        last_ts = last_oplog_entry['ts']
        assert (test_oplog.get_last_oplog_timestamp() == last_ts)

        # test_oplog.stop()

    def test_dump_collection(self):
        """Test dump_collection in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, search_ts, solr, mongos = self.get_oplog_thread()

        # with documents
        retry_until_ok(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        test_oplog.dump_collection()

        docman = test_oplog.doc_managers[0]
        docman.commit()
        solr_results = docman._search()
        assert (len(solr_results) == 1)
        solr_doc = solr_results[0]
        assert (long_to_bson_ts(solr_doc['_ts']) == search_ts)
        assert (solr_doc['name'] == 'paulie')
        assert (solr_doc['ns'] == 'alpha.foo')

    def test_init_cursor(self):
        """Test init_cursor in oplog_manager.

        Assertion failure if it doesn't pass
        """

        test_oplog, search_ts, cursor, mongos = self.get_oplog_thread()
        test_oplog.checkpoint = None           # needed for these tests

        # initial tests with no config file and empty oplog
        assert (test_oplog.init_cursor() is None)

        # no config, single oplog entry
        retry_until_ok(mongos['alpha']['foo'].insert, {'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.init_cursor()

        assert (cursor.count() == 1)
        assert (test_oplog.checkpoint == search_ts)

        # with config file, assert that size != 0
        os.system('touch %s' % (TEMP_CONFIG))

        cursor = test_oplog.init_cursor()
        oplog_dict = test_oplog.oplog_progress.get_dict()

        assert(cursor.count() == 1)
        self.assertTrue(str(test_oplog.oplog) in oplog_dict)
        commit_ts = test_oplog.checkpoint
        self.assertTrue(oplog_dict[str(test_oplog.oplog)] == commit_ts)

        os.system('rm %s' % (TEMP_CONFIG))

    def test_rollback(self):
        """Test rollback in oplog_manager. Assertion failure if it doesn't pass
            We force a rollback by inserting a doc, killing primary, inserting
            another doc, killing the new primary, and then restarting both
            servers.
        """

        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        if not start_cluster(sharded=True):
            self.fail("Shards cannot be added to mongos")

        test_oplog, primary_conn, solr, mongos = self.get_new_oplog()

        solr = test_oplog.doc_managers[0]
        solr._delete()          # equivalent to solr.delete(q='*:*')

        retry_until_ok(mongos['alpha']['foo'].remove, {})
        retry_until_ok(mongos['alpha']['foo'].insert,
                       {'_id': ObjectId('4ff74db3f646462b38000001'),
                        'name': 'paulie'})
        cutoff_ts = test_oplog.get_last_oplog_timestamp()

        obj2 = ObjectId('4ff74db3f646462b38000002')
        first_doc = {'name': 'paulie', '_ts': bson_ts_to_long(cutoff_ts),
                     'ns': 'alpha.foo', 
                     '_id': ObjectId('4ff74db3f646462b38000001')}

        # try kill one, try restarting
        kill_mongo_proc(primary_conn.host, PORTS_ONE['PRIMARY'])

        new_primary_conn = MongoClient(HOSTNAME, int(PORTS_ONE['SECONDARY']))
        admin_db = new_primary_conn['admin']
        while admin_db.command("isMaster")['ismaster'] is False:
            time.sleep(1)
        time.sleep(5)
        count = 0
        while True:
            try:
                mongos['alpha']['foo'].insert({'_id': obj2, 'name': 'paul'})
                break
            except OperationFailure:
                time.sleep(1)
                count += 1
                if count > 60:
                    self.fail('Insert failed too many times in rollback')
                continue

        kill_mongo_proc(primary_conn.host, PORTS_ONE['SECONDARY'])

        start_mongo_proc(PORTS_ONE['PRIMARY'], "demo-repl", "/replset1a",
                       "/replset1a.log", None)

        # wait for master to be established
        while primary_conn['admin'].command("isMaster")['ismaster'] is False:
            time.sleep(1)

        start_mongo_proc(PORTS_ONE['SECONDARY'], "demo-repl", "/replset1b",
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

        solr.upsert(first_doc)
        solr.upsert(second_doc)
        test_oplog.rollback()
        solr.commit()
        results = solr._search()

        self.assertEqual(len(results), 1)

        results_doc = results[0]
        self.assertEqual(results_doc['name'], 'paulie')
        self.assertTrue(results_doc['_ts'] <= bson_ts_to_long(cutoff_ts))


if __name__ == '__main__':
    unittest.main()
