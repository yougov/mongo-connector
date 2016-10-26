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

sys.path[0:0] = [""]

from mongo_connector.connector import Connector
from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.test_utils import (assert_soon,
                                        db_user,
                                        db_password,
                                        ShardedClusterSingle)
from tests import unittest, SkipTest


class TestConnectorSharded(unittest.TestCase):

    def setUp(self):
        if db_user and db_password:
            auth_args = dict(auth_username=db_user, auth_key=db_password)
        else:
            auth_args = {}
        self.cluster = ShardedClusterSingle().start()
        self.dm = DocManager()
        self.connector = Connector(
            mongo_address=self.cluster.uri,
            doc_managers=[self.dm],
            **auth_args
        )
        self.connector.start()

    def tearDown(self):
        self.connector.join()
        try:
            os.unlink('oplog.timestamp')
        except OSError:
            pass
        self.cluster.stop()


class TestConnectorShardedMongosFailure(TestConnectorSharded):

    def test_mongos_connection_failure(self):
        """Test that the connector handles temporary mongos failure"""
        client = self.cluster.client()
        coll = client.test.test
        coll.insert_one({'doc': 1})
        assert_soon(lambda: len(self.dm._search()) == 1)

        # Temporarily shutdown the connector mongos connection
        self.connector.main_conn.close()
        time.sleep(5)
        coll.insert_one({'doc': 2})
        assert_soon(lambda: len(self.dm._search()) == 2)

        # Bring mongos back online
        self.connector.main_conn = self.connector.create_authed_client()
        coll.insert_one({'doc': 3})
        assert_soon(lambda: len(self.dm._search()) == 3)
        client.close()


class TestConnectorShardedAuth(TestConnectorSharded):

    def setUp(self):
        if not (db_user and db_password):
            raise SkipTest('Need to set a user/password to test this.')
        super(TestConnectorShardedAuth, self).setUp()

    def test_start_with_auth(self):
        # Insert some documents into the sharded cluster.  These
        # should go to the DocManager, and the connector should not
        # have an auth failure.
        self.cluster.client().test.test.insert_one({'auth_failure': False})
        assert_soon(lambda: len(self.dm._search()) > 0)


if __name__ == '__main__':
    unittest.main()
