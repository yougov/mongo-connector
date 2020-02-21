# Copyright 2013-2016 MongoDB, Inc.
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

import os
import sys
import time

from pymongo import MongoClient, ReadPreference

sys.path[0:0] = [""]  # noqa

from mongo_connector.connector import Connector, get_mininum_mongodb_version
from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.test_utils import (
    assert_soon,
    db_user,
    db_password,
    ShardedClusterSingle,
)
from mongo_connector.version import Version

from tests import unittest, SkipTest


class ShardedConnectorTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if db_user and db_password:
            auth_args = dict(auth_username=db_user, auth_key=db_password)
        else:
            auth_args = {}
        cls.cluster = ShardedClusterSingle().start()
        cls.main_uri = cls.cluster.uri + "/?readPreference=primaryPreferred"
        cls.dm = DocManager()
        cls.connector = Connector(
            mongo_address=cls.main_uri, doc_managers=[cls.dm], **auth_args
        )
        cls.connector.start()
        assert_soon(
            lambda: len(cls.connector.shard_set) == 2,
            message="connector failed to find both shards!",
        )

    @classmethod
    def tearDownClass(cls):
        cls.connector.join()
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        cls.cluster.stop()


class TestConnectorSharded(ShardedConnectorTestCase):
    def test_connector(self):
        """Test whether the connector initiates properly
        """
        # Make sure get_mininum_mongodb_version returns the current version.
        self.assertEqual(
            Version.from_client(self.cluster.client()), get_mininum_mongodb_version()
        )

    def test_mongos_connection_failure(self):
        """Test that the connector handles temporary mongos failure"""
        client = self.cluster.client()
        coll = client.test.test
        coll.insert_one({"doc": 1})
        assert_soon(lambda: len(self.dm._search()) == 1)

        # Temporarily shutdown the connector mongos connection
        self.connector.main_conn.close()
        time.sleep(5)
        coll.insert_one({"doc": 2})
        assert_soon(lambda: len(self.dm._search()) == 2)

        # Bring mongos back online
        self.connector.main_conn = self.connector.create_authed_client()
        coll.insert_one({"doc": 3})
        assert_soon(lambda: len(self.dm._search()) == 3)
        client.close()

    def test_connector_uri_options(self):
        """Ensure the connector passes URI options to newly created clients.
        """
        expected_client = MongoClient(self.main_uri)

        def assert_client_options(client):
            self.assertEqual(client.read_preference, expected_client.read_preference)
            self.assertEqual(client.read_preference, ReadPreference.PRIMARY_PREFERRED)

        assert_client_options(self.connector.create_authed_client())
        assert_client_options(self.connector.main_conn)
        for oplog_thread in self.connector.shard_set.values():
            assert_client_options(oplog_thread.primary_client)


class TestConnectorShardedAuth(ShardedConnectorTestCase):
    @classmethod
    def setUp(cls):
        if not (db_user and db_password):
            raise SkipTest("Need to set a user/password to test this.")
        super(TestConnectorShardedAuth, cls).setUpClass()

    def test_start_with_auth(self):
        # Insert some documents into the sharded cluster.  These
        # should go to the DocManager, and the connector should not
        # have an auth failure.
        self.cluster.client().test.test.insert_one({"auth_failure": False})
        assert_soon(lambda: len(self.dm._search()) > 0)


if __name__ == "__main__":
    unittest.main()
