# Copyright 2013-2014 MongoDB, Inc.
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

"""Tests methods for mongo_connector
"""

import json
import os
import sys

from bson.timestamp import Timestamp

sys.path[0:0] = [""]  # noqa

from mongo_connector.connector import Connector, get_mininum_mongodb_version
from mongo_connector.test_utils import (
    ReplicaSetSingle,
    connector_opts,
    assert_soon,
    db_user,
    db_password,
)
from mongo_connector.util import long_to_bson_ts
from mongo_connector.version import Version

from tests import unittest, SkipTest


class TestMongoConnector(unittest.TestCase):
    """ Test Class for the Mongo Connector
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the cluster
        """
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        open("oplog.timestamp", "w").close()
        cls.repl_set = ReplicaSetSingle().start()

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        cls.repl_set.stop()

    def test_connector(self):
        """Test whether the connector initiates properly
        """
        conn = Connector(mongo_address=self.repl_set.uri, **connector_opts)
        conn.start()
        assert_soon(lambda: bool(conn.shard_set))

        # Make sure get_mininum_mongodb_version returns the current version.
        self.assertEqual(
            Version.from_client(self.repl_set.client()), get_mininum_mongodb_version()
        )

        conn.join()

        # Make sure the connector is shutdown correctly
        self.assertFalse(conn.can_run)
        for thread in conn.shard_set.values():
            self.assertFalse(thread.running)

    def test_copy_uri_options(self):
        """Test copy_uri_options returns proper MongoDB URIs."""
        uri = "mongodb://host:27017/db?maxPoolSize=1234&w=2"
        self.assertEqual(
            Connector.copy_uri_options("a:123,[::1]:321", uri),
            "mongodb://a:123,[::1]:321/?maxPoolSize=1234&w=2",
        )
        uri = "host:27017"
        self.assertEqual(
            Connector.copy_uri_options("a:123,[::1]:321", uri),
            "mongodb://a:123,[::1]:321",
        )

    def test_write_oplog_progress(self):
        """Test write_oplog_progress under several circumstances
        """
        try:
            os.unlink("temp_oplog.timestamp")
        except OSError:
            pass
        open("temp_oplog.timestamp", "w").close()
        conn = Connector(
            mongo_address=self.repl_set.uri,
            oplog_checkpoint="temp_oplog.timestamp",
            **connector_opts
        )

        # test that None is returned if there is no config file specified.
        self.assertEqual(conn.write_oplog_progress(), None)

        conn.oplog_progress.get_dict()[1] = Timestamp(12, 34)
        # pretend to insert a thread/timestamp pair
        conn.write_oplog_progress()

        data = json.load(open("temp_oplog.timestamp", "r"))
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(12, 34))

        # ensure the temp file was deleted
        self.assertFalse(os.path.exists("temp_oplog.timestamp" + "~"))

        # ensure that updates work properly
        conn.oplog_progress.get_dict()[1] = Timestamp(44, 22)
        conn.write_oplog_progress()

        config_file = open("temp_oplog.timestamp", "r")
        data = json.load(config_file)
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(44, 22))

        config_file.close()
        os.unlink("temp_oplog.timestamp")

    def test_read_oplog_progress(self):
        """Test read_oplog_progress
        """

        conn = Connector(
            mongo_address=self.repl_set.uri, oplog_checkpoint=None, **connector_opts
        )

        # testing with no file
        self.assertEqual(conn.read_oplog_progress(), None)

        try:
            os.unlink("temp_oplog.timestamp")
        except OSError:
            pass
        open("temp_oplog.timestamp", "w").close()

        conn.oplog_checkpoint = "temp_oplog.timestamp"

        # testing with empty file
        self.assertEqual(conn.read_oplog_progress(), None)

        oplog_dict = conn.oplog_progress.get_dict()

        # add a value to the file, delete the dict, and then read in the value
        oplog_dict["oplog1"] = Timestamp(12, 34)
        conn.write_oplog_progress()
        del oplog_dict["oplog1"]

        self.assertEqual(len(oplog_dict), 0)

        conn.read_oplog_progress()
        oplog_dict = conn.oplog_progress.get_dict()

        self.assertTrue("oplog1" in oplog_dict.keys())
        self.assertTrue(oplog_dict["oplog1"], Timestamp(12, 34))

        oplog_dict["oplog1"] = Timestamp(55, 11)

        # see if oplog progress dict is properly updated
        conn.read_oplog_progress()
        self.assertTrue(oplog_dict["oplog1"], Timestamp(55, 11))

        os.unlink("temp_oplog.timestamp")

    def test_connector_minimum_privileges(self):
        """Test the Connector works with a user with minimum privileges."""
        if not (db_user and db_password):
            raise SkipTest("Need to set a user/password to test this.")
        client = self.repl_set.client()
        minimum_user = "read_local_and_included_databases"
        minimum_pwd = "password"
        client.admin.add_user(
            minimum_user,
            minimum_pwd,
            roles=[
                {"role": "read", "db": "test"},
                {"role": "read", "db": "wildcard"},
                {"role": "read", "db": "local"},
            ],
        )

        client.test.test.insert_one({"replicated": 1})
        client.test.ignored.insert_one({"replicated": 0})
        client.ignored.ignored.insert_one({"replicated": 0})
        client.wildcard.test.insert_one({"replicated": 1})
        conn = Connector(
            mongo_address=self.repl_set.primary.uri,
            auth_username=minimum_user,
            auth_key=minimum_pwd,
            namespace_options={"test.test": True, "wildcard.*": True},
        )
        conn.start()
        try:
            assert_soon(conn.doc_managers[0]._search)
        finally:
            conn.join()


if __name__ == "__main__":
    unittest.main()
