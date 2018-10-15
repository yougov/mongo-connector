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

import json
import logging
import os
import re
import ssl
import sys

import pymongo

sys.path[0:0] = [""]  # noqa

from mongo_connector import config, errors, connector
from mongo_connector.connector import get_config_options, setup_logging
from mongo_connector.doc_managers import doc_manager_simulator

from tests import unittest


def from_here(*paths):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *paths)


class TestConfig(unittest.TestCase):
    """Test parsing a JSON config file into a Config object."""

    def setUp(self):
        self.reset_config()

    def reset_config(self):
        self.options = get_config_options()
        self.conf = config.Config(self.options)

    def load_json(self, d, validate=True, reset_config=True):
        if reset_config:
            self.reset_config()
        # Serialize a python dictionary to json, then load it
        text = json.dumps(d)
        self.conf.load_json(text)
        if validate:
            self.load_options(reset_config=False)

    def load_options(self, d={}, reset_config=True):
        if reset_config:
            self.reset_config()
        argv = []
        for k, v in d.items():
            argv.append(str(k))
            if v is not None:
                argv.append(str(v))
        self.conf.parse_args(argv)

    def test_default(self):
        # Make sure default configuration doesn't raise any exceptions
        self.load_options()

    def test_parse_json(self):
        # Test for basic json parsing correctness
        test_config = {
            "mainAddress": u"testMainAddress",
            "oplogFile": u"testOplogFile",
            "noDump": True,
            "batchSize": 69,
            "verbosity": 3,
            "logging": {
                "type": u"file",
                "filename": u"testFilename",
                "format": u"%(asctime)s [%(levelname)s] %(name)s:%(lineno)d"
                u" - %(message)s",
                "rotationWhen": u"midnight",
                "rotationInterval": 1,
                "rotationBackups": 7,
                "host": u"testHost",
                "facility": u"testFacility",
            },
            "authentication": {
                "adminUsername": u"testAdminUsername",
                "password": u"testPassword",
                "passwordFile": u"testPasswordFile",
            },
            "fields": [u"testFields1", u"testField2"],
            "namespaces": {
                "include": [u"testNamespaceSet"],
                "exclude": [u"testExcludeNamespaceSet"],
                "mapping": {"testMapKey": u"testMapValue"},
                "gridfs": [u"testGridfsSet"],
            },
        }
        self.load_json(test_config, validate=False)

        for test_key in test_config:
            self.assertEqual(self.conf[test_key], test_config[test_key])

        # Test for partial dict updates
        test_config = {
            "logging": {"type": "syslog", "host": "testHost2"},
            "authentication": {
                "adminUsername": "testAdminUsername2",
                "passwordFile": "testPasswordFile2",
            },
            "namespaces": {"exclude": [], "mapping": {}},
        }
        self.load_json(test_config, validate=False, reset_config=False)
        self.assertEqual(
            self.conf["logging"],
            {
                "type": u"syslog",
                "filename": u"testFilename",
                "format": u"%(asctime)s [%(levelname)s] %(name)s:%(lineno)d"
                u" - %(message)s",
                "rotationWhen": u"midnight",
                "rotationInterval": 1,
                "rotationBackups": 7,
                "host": u"testHost2",
                "facility": u"testFacility",
            },
        )
        self.assertEqual(
            self.conf["authentication"],
            {
                "adminUsername": u"testAdminUsername2",
                "password": u"testPassword",
                "passwordFile": u"testPasswordFile2",
            },
        )
        self.assertEqual(
            self.conf["namespaces"],
            {
                "include": [u"testNamespaceSet"],
                "exclude": [],
                "mapping": {},
                "gridfs": [u"testGridfsSet"],
            },
        )

    def test_basic_options(self):
        # Test the assignment of individual options
        def test_option(arg_name, json_key, value, append_cli=True):
            self.load_options({arg_name: value if append_cli else None})
            self.assertEqual(self.conf[json_key], value)

        test_option("-m", "mainAddress", "testMainAddress")
        test_option("-o", "oplogFile", "testOplogFileShort")
        test_option("--batch-size", "batchSize", 69)
        test_option("--continue-on-error", "continueOnError", True, append_cli=False)
        test_option("-v", "verbosity", 3, append_cli=False)

        self.load_options({"-w": "logFile"})
        self.assertEqual(self.conf["logging.type"], "file")
        self.assertEqual(self.conf["logging.filename"], os.path.abspath("logFile"))

        self.load_options(
            {
                "-s": None,
                "--syslog-host": "testHost",
                "--syslog-facility": "testFacility",
            }
        )
        self.assertEqual(self.conf["logging.type"], "syslog")
        self.assertEqual(self.conf["logging.host"], "testHost")
        self.assertEqual(self.conf["logging.facility"], "testFacility")

        self.load_options({"-i": "a,b,c"})
        self.assertEqual(self.conf["fields"], ["a", "b", "c"])

    def test_extraneous_command_line_options(self):
        self.assertRaises(errors.InvalidConfiguration, self.load_options, {"-v": 3})
        # No error.
        self.load_options({"-v": None})

    def test_namespace_set(self):
        # test namespace_set and dest_namespace_set
        self.load_options(
            {
                "-n": "source_db_1.col,source_db_2.col,source_db_3.col",
                "-g": "dest_db_1.col,dest_db_2.col,dest_db_3.col",
            }
        )
        self.assertEqual(
            self.conf["namespaces.include"],
            ["source_db_1.col", "source_db_2.col", "source_db_3.col"],
        )
        self.assertEqual(
            self.conf["namespaces.mapping"],
            {
                "source_db_1.col": "dest_db_1.col",
                "source_db_2.col": "dest_db_2.col",
                "source_db_3.col": "dest_db_3.col",
            },
        )

        # test exclude_namespace_set
        self.load_options({"-x": "source_db_1.col,source_db_2.col,source_db_3.col"})
        self.assertEqual(
            self.conf["namespaces.exclude"],
            ["source_db_1.col", "source_db_2.col", "source_db_3.col"],
        )

    def test_namespace_set_validation(self):
        # duplicate ns_set
        args = {"-n": "a.x,a.x,b.y", "-g": "1.0,2.0,3.0"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)
        d = {"namespaces": {"include": ["a.x", "a.x", "b.y"]}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)

        # duplicate ex_ns_set
        args = {"-x": "a.x,a.x,b.y"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)
        d = {"namespaces": {"exclude": ["a.x", "a.x", "b.y"]}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)

        # duplicate gridfs_set
        args = {"--gridfs-set": "a.x,a.x,b.y"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)
        d = {"namespaces": {"gridfs": ["a.x", "a.x", "b.y"]}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)

        # duplicate dest_ns_set
        args = {"-n": "a.x,b.y,c.z", "--dest-namespace-set": "1.0,3.0,3.0"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)
        d = {"namespaces": {"mapping": {"a.x": "c.z", "b.y": "c.z"}}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)

        # len(ns_set) < len(dest_ns_set)
        args = {"--namespace-set": "a.x,b.y,c.z", "-g": "1.0,2.0,3.0,4.0"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

        # len(ns_set) > len(dest_ns_set)
        args = {
            "--namespace-set": "a.x,b.y,c.z,d.j",
            "--dest-namespace-set": "1.0,2.0,3.0",
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

        # validate ns_set format
        args = {"-n": "a*.x*"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)
        d = {"namespaces": {"include": ["a*.x*"]}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)

        # validate dest_ns_set format
        args = {"-n": "a.x*", "-g": "1*.0*"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)
        d = {"namespaces": {"mapping": {"a*.x*": "1.0"}}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)
        d = {"namespaces": {"mapping": {"a.x*": "1*.0*"}}}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, d)

    def test_validate_mixed_namespace_format(self):
        # It is invalid to combine new and old namespace formats
        mix_namespaces = [
            {"mapping": {"old.format": "old.format2"}, "new.format": True},
            {"gridfs": ["old.format"], "new.format": True},
            {"include": ["old.format"], "new.format": True},
            {"exclude": ["old.format"], "new.format": True},
        ]
        for namespaces in mix_namespaces:
            with self.assertRaises(errors.InvalidConfiguration):
                self.load_json({"namespaces": namespaces})

    def test_doc_managers_from_args(self):
        # Test basic docmanager construction from args
        args = {
            "-d": "doc_manager_simulator",
            "-t": "test_target_url",
            "-u": "id",
            "--auto-commit-interval": 10,
        }
        self.load_options(args)
        self.assertEqual(len(self.conf["docManagers"]), 1)

        dm = self.conf["docManagers"][0]
        self.assertTrue(isinstance(dm, doc_manager_simulator.DocManager))
        self.assertEqual(dm.url, "test_target_url")
        self.assertEqual(dm.unique_key, "id")
        self.assertEqual(dm.auto_commit_interval, 10)

        # no doc_manager but target_url
        args = {"-t": "1,2"}
        self.assertRaises(errors.InvalidConfiguration, self.load_options, args)

    def test_config_validation(self):
        # can't log both to syslog and to logfile
        self.assertRaises(
            errors.InvalidConfiguration,
            self.load_options,
            {"-w": "logFile", "-s": "true"},
        )

        # Can't specify --stdout and logfile
        self.assertRaises(
            errors.InvalidConfiguration,
            self.load_options,
            {"--stdout": None, "-w": "logFile"},
        )

        # can't specify a username without a password
        self.assertRaises(
            errors.InvalidConfiguration, self.load_options, {"-a": "username"}
        )

        # can't specify password and password file
        self.assertRaises(
            errors.InvalidConfiguration,
            self.load_options,
            {"-a": "username", "-p": "password", "-f": "password_file"},
        )

        # docManagers must be a list
        test_config = {"docManagers": "hello"}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, test_config)

        # every element of docManagers must contain a 'docManager' property
        test_config = {"docManagers": [{"targetURL": "testTargetURL"}]}
        self.assertRaises(errors.InvalidConfiguration, self.load_json, test_config)

        # auto commit interval can't be negative
        test_config = {
            "docManagers": [{"docManager": "testDocManager", "autoCommitInterval": -1}]
        }
        self.assertRaises(errors.InvalidConfiguration, self.load_json, test_config)

    def test_ssl_validation(self):
        """Test setting sslCertificatePolicy."""
        # Setting sslCertificatePolicy to not 'ignored' without a CA file
        # PyMongo will attempt to load system provided CA certificates.
        for ssl_cert_req in ["required", "optional"]:
            no_ca_config = {"ssl": {"sslCertificatePolicy": ssl_cert_req}}
            if pymongo.version_tuple < (3, 0):
                self.assertRaises(
                    errors.InvalidConfiguration, self.load_json, no_ca_config
                )
            else:
                self.load_json(no_ca_config)
        # Setting sslCertificatePolicy to an invalid option
        self.assertRaises(
            errors.InvalidConfiguration,
            self.load_json,
            {"ssl": {"sslCACerts": "ca.pem", "sslCertificatePolicy": "invalid"}},
        )


class TestConnectorConfig(unittest.TestCase):
    """Test creating a Connector from a Config."""

    # Configuration where every option is set to a non-default value.
    set_everything_config = {
        "mainAddress": "localhost:12345",
        "oplogFile": from_here("lib", "dummy.timestamp"),
        "noDump": True,
        "batchSize": 3,
        "verbosity": 1,
        "continueOnError": True,
        "timezoneAware": True,
        "logging": {
            "type": "file",
            "filename": from_here("lib", "dummy-connector.log"),
            "rotationWhen": "H",
            "rotationInterval": 3,
            "rotationBackups": 10,
        },
        "authentication": {
            "adminUsername": "elmo",
            "passwordFile": from_here("lib", "dummy.pwd"),
        },
        "ssl": {
            "sslCertfile": "certfile.pem",
            "sslKeyfile": "certfile.key",
            "sslCACerts": "ca.pem",
            "sslCertificatePolicy": "optional",
        },
        "fields": ["field1", "field2", "field3"],
        "namespaces": {
            "include": ["db.source1", "db.source2"],
            "mapping": {"db.source1": "db.dest1", "db.source2": "db.dest2"},
            "gridfs": ["db.fs"],
        },
        "docManagers": [
            {
                "docManager": "doc_manager_simulator",
                "targetURL": "localhost:12345",
                "bulkSize": 500,
                "uniqueKey": "id",
                "autoCommitInterval": 10,
                "args": {"key": "value", "clientOptions": {"foo": "bar"}},
            }
        ],
    }

    # Argv that sets all possible options to a different value from the
    # config JSON above. Some options cannot be reset, since they conflict
    # with the JSON config and will cause an Exception to be raised.
    # Conflicted options are already tested in the TestConfig TestCase.
    set_everything_differently_argv = [
        "-m",
        "localhost:1000",
        "-o",
        from_here("lib", "bar.timestamp"),
        "--batch-size",
        "100",
        "--verbose",
        "--logfile-when",
        "D",
        "--logfile-interval",
        "5",
        "--logfile-backups",
        "10",
        "--fields",
        "fieldA,fieldB",
        "--gridfs-set",
        "db.gridfs",
        "--unique-key",
        "customer_id",
        "--auto-commit-interval",
        "100",
        "--continue-on-error",
        "-t",
        "localhost:54321",
        "-d",
        "doc_manager_simulator",
        "-n",
        "foo.bar,fiz.biz",
        "-g",
        "foo2.bar2,fiz2.biz2",
        "--ssl-certfile",
        "certfile2.pem",
        "--ssl-ca-certs",
        "ca2.pem",
        "--ssl-certificate-policy",
        "ignored",
    ]

    # Set of files to keep in the 'lib' directory after each run.
    # The Connector and OplogThread create their own files in this directory
    # that should be cleaned out between tests.
    files_to_keep = set(("dummy.pwd",))

    def setUp(self):
        self.config = config.Config(get_config_options())
        # Remove all logging Handlers, since tests may create Handlers.
        logger = logging.getLogger()
        for handler in logger.handlers:
            logger.removeHandler(handler)

    def tearDown(self):
        for filename in os.listdir(from_here("lib")):
            if filename not in self.files_to_keep:
                try:
                    os.remove(from_here("lib", filename))
                except OSError:
                    pass  # File may no longer exist.

    def assertConnectorState(self):
        """Assert that a Connector is constructed from a Config properly."""
        mc = connector.Connector.from_config(self.config)

        # Test Connector options.
        self.assertEqual(mc.address, self.config["mainAddress"])
        self.assertIsInstance(mc.doc_managers[0], doc_manager_simulator.DocManager)

        pwfile = self.config["authentication.passwordFile"]
        if pwfile:
            with open(pwfile, "r") as fd:
                test_password = re.sub(r"\s", "", fd.read())
                self.assertEqual(mc.auth_key, test_password)

        self.assertEqual(mc.auth_username, self.config["authentication.adminUsername"])
        self.assertEqual(mc.oplog_checkpoint, self.config["oplogFile"])
        self.assertEqual(mc.tz_aware, self.config["timezoneAware"])
        self.assertEqual(
            mc.ssl_kwargs.get("ssl_certfile"), self.config["ssl.sslCertfile"]
        )
        self.assertEqual(
            mc.ssl_kwargs.get("ssl_ca_certs"), self.config["ssl.sslCACerts"]
        )
        self.assertEqual(
            mc.ssl_kwargs.get("ssl_keyfile"), self.config["ssl.sslKeyfile"]
        )
        self.assertEqual(
            mc.ssl_kwargs.get("ssl_cert_reqs"), self.config["ssl.sslCertificatePolicy"]
        )
        command_helper = mc.doc_managers[0].command_helper
        for name in self.config["namespaces.mapping"]:
            self.assertTrue(command_helper.namespace_config.map_namespace(name))

        # Test Logger options.
        log_levels = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
        test_logger = setup_logging(self.config)
        self.assertEqual(log_levels[self.config["verbosity"]], test_logger.level)
        test_handlers = [
            h
            for h in test_logger.handlers
            if isinstance(h, logging.handlers.TimedRotatingFileHandler)
        ]
        self.assertEqual(len(test_handlers), 1)
        test_handler = test_handlers[0]
        expected_handler = logging.handlers.TimedRotatingFileHandler(
            "test-dummy.log",
            when=self.config["logging.rotationWhen"],
            interval=self.config["logging.rotationInterval"],
            backupCount=self.config["logging.rotationBackups"],
        )
        self.assertEqual(test_handler.when, expected_handler.when)
        self.assertEqual(test_handler.backupCount, expected_handler.backupCount)
        self.assertEqual(test_handler.interval, expected_handler.interval)

        # Test keyword arguments passed to OplogThread.
        ot_kwargs = mc.kwargs
        self.assertEqual(ot_kwargs["ns_set"], self.config["namespaces.include"])
        self.assertEqual(ot_kwargs["collection_dump"], not self.config["noDump"])
        self.assertEqual(ot_kwargs["gridfs_set"], self.config["namespaces.gridfs"])
        self.assertEqual(ot_kwargs["continue_on_error"], self.config["continueOnError"])
        self.assertEqual(ot_kwargs["fields"], self.config["fields"])
        self.assertEqual(ot_kwargs["batch_size"], self.config["batchSize"])

        # Test DocManager options.
        for dm, dm_expected in zip(mc.doc_managers, self.config["docManagers"]):
            self.assertEqual(dm.kwargs, dm_expected.kwargs)
            self.assertEqual(dm.auto_commit_interval, dm_expected.auto_commit_interval)
            self.assertEqual(dm.url, dm_expected.url)
            self.assertEqual(dm.chunk_size, dm_expected.chunk_size)

    def test_connector_config_file_options(self):
        # Test Config with only a configuration file.
        self.config.load_json(json.dumps(self.set_everything_config))
        self.config.parse_args(argv=[])
        self.assertConnectorState()

    def test_connector_with_argv(self):
        # Test Config with arguments given on the command-line.
        self.config.parse_args(self.set_everything_differently_argv)
        self.assertConnectorState()

    def test_override_config_with_argv(self):
        # Override some options in the config file with given command-line
        # options.
        self.config.load_json(json.dumps(TestConnectorConfig.set_everything_config))
        self.config.parse_args(self.set_everything_differently_argv)

        first_dm = self.config["docManagers"][0]
        first_dm_config = self.set_everything_config["docManagers"][0]
        self.assertEqual(first_dm.url, "localhost:54321")
        self.assertEqual(first_dm.chunk_size, first_dm_config["bulkSize"])
        self.assertEqual(
            first_dm.kwargs.get("clientOptions"),
            first_dm_config["args"]["clientOptions"],
        )
        self.assertConnectorState()

    def test_client_options(self):
        config_def = {
            "mainAddress": "localhost:27017",
            "oplogFile": from_here("lib", "dummy.timestamp"),
            "docManagers": [
                {
                    "docManager": "mongo_doc_manager",
                    "targetURL": "dummyhost:27017",
                    "args": {"clientOptions": {"maxPoolSize": 50, "connect": False}},
                }
            ],
        }
        config_obj = config.Config(get_config_options())
        config_obj.load_json(json.dumps(config_def))
        config_obj.parse_args(argv=[])
        conn = connector.Connector.from_config(config_obj)
        self.assertEqual(50, conn.doc_managers[0].mongo.max_pool_size)

    def test_ssl_options(self):
        config_def = {
            "mainAddress": "localhost:27017",
            "oplogFile": from_here("lib", "dummy.timestamp"),
            "ssl": {
                "sslCertfile": "certfile.pem",
                "sslKeyfile": "certfile.key",
                "sslCACerts": "ca.pem",
            },
        }
        for cert_policy, expected_ssl_cert_req in [
            ("ignored", ssl.CERT_NONE),
            ("optional", ssl.CERT_OPTIONAL),
            ("required", ssl.CERT_REQUIRED),
            (None, None),
        ]:
            config_def["ssl"]["sslCertificatePolicy"] = cert_policy
            config_obj = config.Config(get_config_options())
            config_obj.load_json(json.dumps(config_def))
            config_obj.parse_args(argv=[])
            mc = connector.Connector.from_config(config_obj)
            self.assertEqual("certfile.pem", mc.ssl_kwargs.get("ssl_certfile"))
            self.assertEqual("ca.pem", mc.ssl_kwargs.get("ssl_ca_certs"))
            self.assertEqual("certfile.key", mc.ssl_kwargs.get("ssl_keyfile"))
            self.assertEqual(expected_ssl_cert_req, mc.ssl_kwargs.get("ssl_cert_reqs"))


if __name__ == "__main__":
    unittest.main()
