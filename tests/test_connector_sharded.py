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

from mongo_connector.connector import Connector
from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.test_utils import (ShardedCluster,
                                        db_user,
                                        db_password,
                                        assert_soon)
from tests import unittest, SkipTest


class TestConnectorSharded(unittest.TestCase):

    def setUp(self):
        if not (db_user and db_password):
            raise SkipTest('Need to set a user/password to test this.')
        self.cluster = ShardedCluster().start()

    def tearDown(self):
        try:
            os.unlink('oplog.timestamp')
        except OSError:
            pass
        self.cluster.stop()

    def test_start_with_auth(self):
        dm = DocManager()
        connector = Connector(
            mongo_address=self.cluster.uri,
            doc_managers=[dm],
            auth_username=db_user,
            auth_key=db_password
        )
        connector.start()

        # Insert some documents into the sharded cluster.  These
        # should go to the DocManager, and the connector should not
        # have an auth failure.
        self.cluster.client().test.test.insert_one({'auth_failure': False})
        assert_soon(lambda: len(dm._search()) > 0)

        connector.join()
