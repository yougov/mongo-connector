import json
import logging
import os
import sys

from mongo_connector import config
from mongo_connector.connector import get_config_options, setup_logging
from tests import unittest
from raven.handlers.logging import SentryHandler

__author__ = 'oxymor0n'

sys.path[0:0] = [""]
from_here = lambda *paths: os.path.join(
    os.path.abspath(os.path.dirname(__file__)), *paths)

SENTRY_DSN = "https://b37500744ba24f549ceafa94f0d8e52f:c94be83e51ab442993dcda1f4443be60@app.getsentry.com/59495"


class TestConnectorConfig(unittest.TestCase):
    """Test creating a Connector from a Config."""

    # Configuration where every option is set to a non-default value.
    set_everything_config = {
        "mainAddress": "localhost:12345",
        "oplogFile": from_here("lib", "dummy.timestamp"),
        "noDump": True,
        "batchSize": 3,
        "verbosity": 2,
        "continueOnError": True,
        "timezoneAware": True,

        "logging": {
            "type": "sentry",
            "dsn": SENTRY_DSN,
        },

        "docManagers": [
            {
                "docManager": "doc_manager_simulator",
                "targetURL": "localhost:12345",
                "bulkSize": 500,
                "uniqueKey": "id",
                "autoCommitInterval": 10,
                "args": {
                    "key": "value",
                    "clientOptions": {
                        "foo": "bar"
                    }
                }
            }
        ]
    }

    def setUp(self):
        self.config = config.Config(get_config_options())
        # Remove all logging Handlers, since tests may create Handlers.
        logger = logging.getLogger()
        for handler in logger.handlers:
            logger.removeHandler(handler)

    def assertLoggerState(self):
        """Assert that the logger has a valid Raven SentryHandler."""
        # Test Logger options.
        log_levels = [
            logging.ERROR,
            logging.WARNING,
            logging.INFO,
            logging.DEBUG
        ]
        test_logger = setup_logging(self.config)
        self.assertEqual(
            log_levels[self.config['verbosity']],
            test_logger.level)
        test_handlers = [
            h for h in test_logger.handlers
            if isinstance(h, SentryHandler)]
        self.assertEqual(len(test_handlers), 1)
        test_handler = test_handlers[0]
        expected_handler = SentryHandler(SENTRY_DSN)
        self.assertEqual(test_handler.client.remote.get_public_dsn(), expected_handler.client.remote.get_public_dsn())
        test_logger.info("Hello, Sentry!")

    def test_sentry_logger(self):
        # Test Config with only a configuration file.
        self.config.load_json(
            json.dumps(self.set_everything_config))
        self.config.parse_args(argv=[])
        self.assertLoggerState()


if __name__ == '__main__':
    unittest.main()
