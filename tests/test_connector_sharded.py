import os

from mongo_connector.connector import Connector
from mongo_connector.doc_managers.doc_manager_simulator import DocManager

from tests import unittest, SkipTest, db_user, db_password
from tests.setup_cluster import ShardedCluster
from tests.util import assert_soon


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
        self.cluster.client().test.test.insert({'auth_failure': False})
        assert_soon(lambda: len(dm._search()) > 0)

        connector.join()
