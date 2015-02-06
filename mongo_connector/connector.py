# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Discovers the mongo cluster and starts the connector.
"""

import json
import logging
import logging.handlers
import os
import pymongo
import re
import shutil
import ssl
import sys
import threading
import time
from mongo_connector import config, constants, errors, util
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.doc_managers import doc_manager_simulator as simulator
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.command_helper import CommandHelper
from mongo_connector.util import log_fatal_exceptions

from pymongo import MongoClient

LOG = logging.getLogger(__name__)

_SSL_POLICY_MAP = {
    'ignored': ssl.CERT_NONE,
    'optional': ssl.CERT_OPTIONAL,
    'required': ssl.CERT_REQUIRED
}


class Connector(threading.Thread):
    """Thread that monitors a replica set or sharded cluster.

    Creates, runs, and monitors an OplogThread for each replica set found.
    """
    def __init__(self, mongo_address, doc_managers=None, **kwargs):
        super(Connector, self).__init__()

        # can_run is set to false when we join the thread
        self.can_run = True

        # main address - either mongos for sharded setups or a primary otherwise
        self.address = mongo_address

        # List of DocManager instances
        if doc_managers:
            self.doc_managers = doc_managers
        else:
            LOG.info('No doc managers specified, using simulator.')
            self.doc_managers = (simulator.DocManager(),)

        # Password for authentication
        self.auth_key = kwargs.pop('auth_key', None)

        # Username for authentication
        self.auth_username = kwargs.pop('auth_username', None)

        # The name of the file that stores the progress of the OplogThreads
        self.oplog_checkpoint = kwargs.pop('oplog_checkpoint',
                                           'oplog.timestamp')

        # The set of OplogThreads created
        self.shard_set = {}

        # Dict of OplogThread/timestamp pairs to record progress
        self.oplog_progress = LockingDict()

        # Timezone awareness
        self.tz_aware = kwargs.get('tz_aware', False)

        # SSL keyword arguments to MongoClient.
        ssl_certfile = kwargs.pop('ssl_certfile', None)
        ssl_ca_certs = kwargs.pop('ssl_ca_certs', None)
        ssl_keyfile = kwargs.pop('ssl_keyfile', None)
        ssl_cert_reqs = kwargs.pop('ssl_cert_reqs', None)
        self.ssl_kwargs = {}
        if ssl_certfile:
            self.ssl_kwargs['ssl_certfile'] = ssl_certfile
        if ssl_ca_certs:
            self.ssl_kwargs['ssl_ca_certs'] = ssl_ca_certs
        if ssl_keyfile:
            self.ssl_kwargs['ssl_keyfile'] = ssl_keyfile
        if ssl_cert_reqs:
            self.ssl_kwargs['ssl_cert_reqs'] = ssl_cert_reqs

        # Save the rest of kwargs.
        self.kwargs = kwargs

        # Initialize and set the command helper
        command_helper = CommandHelper(kwargs.get('ns_set', []),
                                       kwargs.get('dest_mapping', {}))
        for dm in self.doc_managers:
            dm.command_helper = command_helper

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                info_str = ("MongoConnector: Can't find %s, "
                            "attempting to create an empty progress log" %
                            self.oplog_checkpoint)
                LOG.info(info_str)
                try:
                    # Create oplog progress file
                    open(self.oplog_checkpoint, "w").close()
                except IOError as e:
                    LOG.critical("MongoConnector: Could not "
                                 "create a progress log: %s" %
                                 str(e))
                    sys.exit(2)
            else:
                if (not os.access(self.oplog_checkpoint, os.W_OK)
                        and not os.access(self.oplog_checkpoint, os.R_OK)):
                    LOG.critical("Invalid permissions on %s! Exiting" %
                                 (self.oplog_checkpoint))
                    sys.exit(2)

    @classmethod
    def from_config(cls, config):
        """Create a new Connector instance from a Config object."""
        auth_key = None
        password_file = config['authentication.passwordFile']
        if password_file is not None:
            try:
                auth_key = open(config['passwordFile']).read()
                auth_key = re.sub(r'\s', '', auth_key)
            except IOError:
                LOG.error('Could not load password file!')
                sys.exit(1)
        password = config['authentication.password']
        if password is not None:
            auth_key = password
        connector = Connector(
            mongo_address=config['mainAddress'],
            doc_managers=config['docManagers'],
            oplog_checkpoint=config['oplogFile'],
            collection_dump=(not config['noDump']),
            batch_size=config['batchSize'],
            continue_on_error=config['continueOnError'],
            auth_username=config['authentication.adminUsername'],
            auth_key=auth_key,
            fields=config['fields'],
            ns_set=config['namespaces.include'],
            dest_mapping=config['namespaces.mapping'],
            gridfs_set=config['namespaces.gridfs'],
            ssl_certfile=config['ssl.sslCertfile'],
            ssl_keyfile=config['ssl.sslKeyfile'],
            ssl_ca_certs=config['ssl.sslCACerts'],
            ssl_cert_reqs=config['ssl.sslCertificatePolicy']
        )
        return connector

    def join(self):
        """ Joins thread, stops it from running
        """
        self.can_run = False
        for dm in self.doc_managers:
            dm.stop()
        threading.Thread.join(self)

    def write_oplog_progress(self):
        """ Writes oplog progress to file provided by user
        """

        if self.oplog_checkpoint is None:
            return None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
        items = [[name, util.bson_ts_to_long(oplog_dict[name])]
                 for name in oplog_dict]
        if not items:
            return

        # write to temp file
        backup_file = self.oplog_checkpoint + '.backup'
        os.rename(self.oplog_checkpoint, backup_file)

        # for each of the threads write to file
        with open(self.oplog_checkpoint, 'w') as dest:
            if len(items) == 1:
                # Write 1-dimensional array, as in previous versions.
                json_str = json.dumps(items[0])
            else:
                # Write a 2d array to support sharded clusters.
                json_str = json.dumps(items)
            try:
                dest.write(json_str)
            except IOError:
                # Basically wipe the file, copy from backup
                dest.truncate()
                with open(backup_file, 'r') as backup:
                    shutil.copyfile(backup, dest)

        os.remove(backup_file)

    def read_oplog_progress(self):
        """Reads oplog progress from file provided by user.
        This method is only called once before any threads are spanwed.
        """

        if self.oplog_checkpoint is None:
            return None

        # Check for empty file
        try:
            if os.stat(self.oplog_checkpoint).st_size == 0:
                LOG.info("MongoConnector: Empty oplog progress file.")
                return None
        except OSError:
            return None

        with open(self.oplog_checkpoint, 'r') as progress_file:
            try:
                data = json.load(progress_file)
            except ValueError:
                LOG.exception(
                    'Cannot read oplog progress file "%s". '
                    'It may be corrupt after Mongo Connector was shut down'
                    'uncleanly. You can try to recover from a backup file '
                    '(may be called "%s.backup") or create a new progress file '
                    'starting at the current moment in time by running '
                    'mongo-connector --no-dump <other options>. '
                    'You may also be trying to read an oplog progress file '
                    'created with the old format for sharded clusters. '
                    'See https://github.com/10gen-labs/mongo-connector/wiki'
                    '/Oplog-Progress-File for complete documentation.'
                    % (self.oplog_checkpoint, self.oplog_checkpoint))
                return
            # data format:
            # [name, timestamp] = replica set
            # [[name, timestamp], [name, timestamp], ...] = sharded cluster
            if not isinstance(data[0], list):
                data = [data]
            with self.oplog_progress:
                self.oplog_progress.dict = dict(
                    (name, util.long_to_bson_ts(timestamp))
                    for name, timestamp in data)

    @log_fatal_exceptions
    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        main_conn = MongoClient(
            self.address, tz_aware=self.tz_aware, **self.ssl_kwargs)
        if self.auth_key is not None:
            main_conn['admin'].authenticate(self.auth_username, self.auth_key)
        self.read_oplog_progress()
        conn_type = None

        try:
            main_conn.admin.command("isdbgrid")
        except pymongo.errors.OperationFailure:
            conn_type = "REPLSET"

        if conn_type == "REPLSET":
            # Make sure we are connected to a replica set
            is_master = main_conn.admin.command("isMaster")
            if "setName" not in is_master:
                LOG.error(
                    'No replica set at "%s"! A replica set is required '
                    'to run mongo-connector. Shutting down...' % self.address
                )
                return

            # Establish a connection to the replica set as a whole
            main_conn.disconnect()
            main_conn = MongoClient(
                self.address, replicaSet=is_master['setName'],
                tz_aware=self.tz_aware, **self.ssl_kwargs)
            if self.auth_key is not None:
                main_conn.admin.authenticate(self.auth_username, self.auth_key)

            # non sharded configuration
            oplog = OplogThread(
                main_conn, self.doc_managers, self.oplog_progress,
                **self.kwargs)
            self.shard_set[0] = oplog
            LOG.info('MongoConnector: Starting connection thread %s' %
                     main_conn)
            oplog.start()

            while self.can_run:
                if not self.shard_set[0].running:
                    LOG.error("MongoConnector: OplogThread"
                              " %s unexpectedly stopped! Shutting down" %
                              (str(self.shard_set[0])))
                    self.oplog_thread_join()
                    for dm in self.doc_managers:
                        dm.stop()
                    return

                self.write_oplog_progress()
                time.sleep(1)

        else:       # sharded cluster
            while self.can_run is True:

                for shard_doc in main_conn['config']['shards'].find():
                    shard_id = shard_doc['_id']
                    if shard_id in self.shard_set:
                        if not self.shard_set[shard_id].running:
                            LOG.error("MongoConnector: OplogThread "
                                      "%s unexpectedly stopped! Shutting "
                                      "down" %
                                      (str(self.shard_set[shard_id])))
                            self.oplog_thread_join()
                            for dm in self.doc_managers:
                                dm.stop()
                            return

                        self.write_oplog_progress()
                        time.sleep(1)
                        continue
                    try:
                        repl_set, hosts = shard_doc['host'].split('/')
                    except ValueError:
                        cause = "The system only uses replica sets!"
                        LOG.exception("MongoConnector: %s", cause)
                        self.oplog_thread_join()
                        for dm in self.doc_managers:
                            dm.stop()
                        return

                    shard_conn = MongoClient(
                        hosts, replicaSet=repl_set, tz_aware=self.tz_aware,
                        **self.ssl_kwargs)
                    oplog = OplogThread(
                        shard_conn, self.doc_managers, self.oplog_progress,
                        **self.kwargs)
                    self.shard_set[shard_id] = oplog
                    msg = "Starting connection thread"
                    LOG.info("MongoConnector: %s %s" % (msg, shard_conn))
                    oplog.start()

        self.oplog_thread_join()
        self.write_oplog_progress()

    def oplog_thread_join(self):
        """Stops all the OplogThreads
        """
        LOG.info('MongoConnector: Stopping all OplogThreads')
        for thread in self.shard_set.values():
            thread.join()


def get_config_options():
    result = []

    def add_option(*args, **kwargs):
        opt = config.Option(*args, **kwargs)
        result.append(opt)
        return opt

    main_address = add_option(
        config_key="mainAddress",
        default="localhost:27017",
        type=str)

    # -m is for the main address, which is a host:port pair, ideally of the
    # mongos. For non sharded clusters, it can be the primary.
    main_address.add_cli(
        "-m", "--main", dest="main_address", help=
        "Specify the main address, which is a"
        " host:port pair. For sharded clusters, this"
        " should be the mongos address. For individual"
        " replica sets, supply the address of the"
        " primary. For example, `-m localhost:27217`"
        " would be a valid argument to `-m`. Don't use"
        " quotes around the address.")

    oplog_file = add_option(
        config_key="oplogFile",
        default="oplog.timestamp",
        type=str)

    # -o is to specify the oplog-config file. This file is used by the system
    # to store the last timestamp read on a specific oplog. This allows for
    # quick recovery from failure.
    oplog_file.add_cli(
        "-o", "--oplog-ts", dest="oplog_file", help=
        "Specify the name of the file that stores the "
        "oplog progress timestamps. "
        "This file is used by the system to store the last "
        "timestamp read on a specific oplog. This allows "
        "for quick recovery from failure. By default this "
        "is `config.txt`, which starts off empty. An empty "
        "file causes the system to go through all the mongo "
        "oplog and sync all the documents. Whenever the "
        "cluster is restarted, it is essential that the "
        "oplog-timestamp config file be emptied - otherwise "
        "the connector will miss some documents and behave "
        "incorrectly.")

    no_dump = add_option(
        config_key="noDump",
        default=False,
        type=bool)

    # --no-dump specifies whether we should read an entire collection from
    # scratch if no timestamp is found in the oplog_config.
    no_dump.add_cli(
        "--no-dump", action="store_true", dest="no_dump", help=
        "If specified, this flag will ensure that "
        "mongo_connector won't read the entire contents of a "
        "namespace iff --oplog-ts points to an empty file.")

    batch_size = add_option(
        config_key="batchSize",
        default=constants.DEFAULT_BATCH_SIZE,
        type=int)

    # --batch-size specifies num docs to read from oplog before updating the
    # --oplog-ts config file with current oplog position
    batch_size.add_cli(
        "--batch-size", type="int", dest="batch_size", help=
        "Specify an int to update the --oplog-ts "
        "config file with latest position of oplog every "
        "N documents. By default, the oplog config isn't "
        "updated until we've read through the entire oplog. "
        "You may want more frequent updates if you are at risk "
        "of falling behind the earliest timestamp in the oplog")

    def apply_verbosity(option, cli_values):
        if cli_values['verbose']:
            option.value = 1
        if option.value < 0:
            raise errors.InvalidConfiguration(
                "verbosity must be non-negative.")

    verbosity = add_option(
        config_key="verbosity",
        default=0,
        type=int,
        apply_function=apply_verbosity)

    # -v enables verbose logging
    verbosity.add_cli(
        "-v", "--verbose", action="store_true",
        dest="verbose", help=
        "Sets verbose logging to be on.")

    def apply_logging(option, cli_values):
        if cli_values['logfile'] and cli_values['enable_syslog']:
            raise errors.InvalidConfiguration(
                "You cannot specify syslog and a logfile simultaneously,"
                " please choose the logging method you would prefer.")

        if cli_values['logfile']:
            when = cli_values['logfile_when']
            interval = cli_values['logfile_interval']
            if (when and when.startswith('W') and
                    interval != constants.DEFAULT_LOGFILE_INTERVAL):
                raise errors.InvalidConfiguration(
                    "You cannot specify a log rotation interval when rotating "
                    "based on a weekday (W0 - W6).")

            option.value['type'] = 'file'
            option.value['filename'] = cli_values['logfile']
            if when:
                option.value['rotationWhen'] = when
            if interval:
                option.value['rotationInterval'] = interval
            if cli_values['logfile_backups']:
                option.value['rotationBackups'] = cli_values['logfile_backups']

        if cli_values['enable_syslog']:
            option.value['type'] = 'syslog'

        if cli_values['syslog_host']:
            option.value['host'] = cli_values['syslog_host']

        if cli_values['syslog_facility']:
            option.value['facility'] = cli_values['syslog_facility']

    default_logging = {
        'type': 'file',
        'filename': 'mongo-connector.log',
        'rotationInterval': constants.DEFAULT_LOGFILE_INTERVAL,
        'rotationBackups': constants.DEFAULT_LOGFILE_BACKUPCOUNT,
        'rotationWhen': constants.DEFAULT_LOGFILE_WHEN,
        'host': constants.DEFAULT_SYSLOG_HOST,
        'facility': constants.DEFAULT_SYSLOG_FACILITY
    }

    logging = add_option(
        config_key="logging",
        default=default_logging,
        type=dict,
        apply_function=apply_logging)

    # -w enables logging to a file
    logging.add_cli(
        "-w", "--logfile", dest="logfile", help=
        "Log all output to a file rather than stream to "
        "stderr. Omit to stream to stderr.")

    # -s is to enable syslog logging.
    logging.add_cli(
        "-s", "--enable-syslog", action="store_true",
        dest="enable_syslog", help=
        "The syslog host, which may be an address like 'localhost:514' or, "
        "on Unix/Linux, the path to a Unix domain socket such as '/dev/log'.")

    # --syslog-host is to specify the syslog host.
    logging.add_cli(
        "--syslog-host", dest="syslog_host", help=
        "Used to specify the syslog host."
        " The default is 'localhost:514'")

    # --syslog-facility is to specify the syslog facility.
    logging.add_cli(
        "--syslog-facility", dest="syslog_facility", help=
        "Used to specify the syslog facility."
        " The default is 'user'")

    # --logfile-when specifies the type of interval of the rotating file
    # (seconds, minutes, hours)
    logging.add_cli("--logfile-when", action="store", dest="logfile_when",
                    type="string",
                    help="The type of interval for rotating the log file. "
                    "Should be one of "
                    "'S' (seconds), 'M' (minutes), 'H' (hours), "
                    "'D' (days), 'W0' - 'W6' (days of the week 0 - 6), "
                    "or 'midnight' (the default). See the Python documentation "
                    "for 'logging.handlers.TimedRotatingFileHandler' for more "
                    "details.")

    # --logfile-interval specifies when to create a new log file
    logging.add_cli("--logfile-interval", action="store",
                    dest="logfile_interval", type="int",
                    help="How frequently to rotate the log file, "
                    "specifically, how many units of the rotation interval "
                    "should pass before the rotation occurs. For example, "
                    "to create a new file each hour: "
                    " '--logfile-when=H --logfile-interval=1'. "
                    "Defaults to 1. You may not use this option if "
                    "--logfile-when is set to a weekday (W0 - W6). "
                    "See the Python documentation for "
                    "'logging.handlers.TimedRotatingFileHandler' for more "
                    "details. ")

    # --logfile-backups specifies how many log files will be kept.
    logging.add_cli("--logfile-backups", action="store",
                    dest="logfile_backups", type="int",
                    help="How many log files will be kept after rotation. "
                    "If set to zero, then no log files will be deleted. "
                    "Defaults to 7.")

    def apply_authentication(option, cli_values):
        if cli_values['admin_username']:
            option.value['adminUsername'] = cli_values['admin_username']

        if cli_values['password']:
            option.value['password'] = cli_values['password']

        if cli_values['password_file']:
            option.value['passwordFile'] = cli_values['password_file']

        if option.value.get("adminUsername"):
            password = option.value.get("password")
            passwordFile = option.value.get("passwordFile")
            if not password and not passwordFile:
                raise errors.InvalidConfiguration(
                    "Admin username specified without password.")
            if password and passwordFile:
                raise errors.InvalidConfiguration(
                    "Can't specify both password and password file.")

    default_authentication = {
        'adminUsername': None,
        'password': None,
        'passwordFile': None
    }

    authentication = add_option(
        config_key="authentication",
        default=default_authentication,
        type=dict,
        apply_function=apply_authentication)

    # -a is to specify the username for authentication.
    authentication.add_cli(
        "-a", "--admin-username", dest="admin_username", help=
        "Used to specify the username of an admin user to "
        "authenticate with. To use authentication, the user "
        "must specify both an admin username and a keyFile.")

    # -p is to specify the password used for authentication.
    authentication.add_cli(
        "-p", "--password", dest="password", help=
        "Used to specify the password."
        " This is used by mongos to authenticate"
        " connections to the shards, and in the"
        " oplog threads. If authentication is not used, then"
        " this field can be left empty as the default ")

    # -f is to specify the authentication key file. This file is used by mongos
    # to authenticate connections to the shards, and we'll use it in the oplog
    # threads.
    authentication.add_cli(
        "-f", "--password-file", dest="password_file", help=
        "Used to store the password for authentication."
        " Use this option if you wish to specify a"
        " username and password but don't want to"
        " type in the password. The contents of this"
        " file should be the password for the admin user.")

    def apply_fields(option, cli_values):
        if cli_values['fields']:
            option.value = cli_values['fields'].split(",")

    fields = add_option(
        config_key="fields",
        default=[],
        type=list,
        apply_function=apply_fields)

    # -i to specify the list of fields to export
    fields.add_cli(
        "-i", "--fields", dest="fields", help=
        "Used to specify the list of fields to export. "
        "Specify a field or fields to include in the export. "
        "Use a comma separated list of fields to specify multiple "
        "fields. The '_id', 'ns' and '_ts' fields are always "
        "exported.")

    def apply_namespaces(option, cli_values):
        if cli_values['ns_set']:
            option.value['include'] = cli_values['ns_set'].split(',')

        if cli_values['gridfs_set']:
            option.value['gridfs'] = cli_values['gridfs_set'].split(',')

        if cli_values['dest_ns_set']:
            ns_set = option.value['include']
            dest_ns_set = cli_values['dest_ns_set'].split(',')
            if len(ns_set) != len(dest_ns_set):
                raise errors.InvalidConfiguration(
                    "Destination namespace set should be the"
                    " same length as the origin namespace set.")
            option.value['mapping'] = dict(zip(ns_set, dest_ns_set))

        ns_set = option.value['include']
        if len(ns_set) != len(set(ns_set)):
            raise errors.InvalidConfiguration(
                "Namespace set should not contain any duplicates.")

        dest_mapping = option.value['mapping']
        if len(dest_mapping) != len(set(dest_mapping.values())):
            raise errors.InvalidConfiguration(
                "Destination namespaces set should not"
                " contain any duplicates.")

        gridfs_set = option.value['gridfs']
        if len(gridfs_set) != len(set(gridfs_set)):
            raise errors.InvalidConfiguration(
                "GridFS set should not contain any duplicates.")

    default_namespaces = {
        "include": [],
        "mapping": {},
        "gridfs": []
    }

    namespaces = add_option(
        config_key="namespaces",
        default=default_namespaces,
        type=dict,
        apply_function=apply_namespaces)

    # -n is to specify the namespaces we want to consider. The default
    # considers all the namespaces
    namespaces.add_cli(
        "-n", "--namespace-set", dest="ns_set", help=
        "Used to specify the namespaces we want to "
        "consider. For example, if we wished to store all "
        "documents from the test.test and alpha.foo "
        "namespaces, we could use `-n test.test,alpha.foo`. "
        "The default is to consider all the namespaces, "
        "excluding the system and config databases, and "
        "also ignoring the \"system.indexes\" collection in "
        "any database.")

    # -g is the destination namespace
    namespaces.add_cli(
        "-g", "--dest-namespace-set", dest="dest_ns_set", help=
        "Specify a destination namespace mapping. Each "
        "namespace provided in the --namespace-set option "
        "will be mapped respectively according to this "
        "comma-separated list. These lists must have "
        "equal length. The default is to use the identity "
        "mapping. This is currently only implemented "
        "for mongo-to-mongo connections.")

    # --gridfs-set is the set of GridFS namespaces to consider
    namespaces.add_cli(
        "--gridfs-set", dest="gridfs_set", help=
        "Used to specify the GridFS namespaces we want to "
        "consider. For example, if your metadata is stored in "
        "test.fs.files and chunks are stored in test.fs.chunks, "
        "you can use `--gridfs-set test.fs`.")

    def apply_doc_managers(option, cli_values):
        if cli_values['doc_manager'] is None:
            if cli_values['target_url']:
                raise errors.InvalidConfiguration(
                    "Cannot create a Connector with a target URL"
                    " but no doc manager.")
        else:
            option.value = [{
                'docManager': cli_values['doc_manager'],
                'targetURL': cli_values['target_url'],
                'uniqueKey': cli_values['unique_key'],
                'autoCommitInterval': cli_values['auto_commit_interval']
            }]

        if not option.value:
            return

        # validate doc managers and fill in default values
        for dm in option.value:
            if not isinstance(dm, dict):
                raise errors.InvalidConfiguration(
                    "Elements of docManagers must be a dict.")
            if 'docManager' not in dm:
                raise errors.InvalidConfiguration(
                    "Every element of docManagers"
                    " must contain 'docManager' property.")
            if not dm.get('targetURL'):
                dm['targetURL'] = None
            if not dm.get('uniqueKey'):
                dm['uniqueKey'] = constants.DEFAULT_UNIQUE_KEY
            if not dm.get('autoCommitInterval'):
                dm['autoCommitInterval'] = constants.DEFAULT_COMMIT_INTERVAL
            if not dm.get('args'):
                dm['args'] = {}

            if dm['autoCommitInterval'] and dm['autoCommitInterval'] < 0:
                raise errors.InvalidConfiguration(
                    "autoCommitInterval must be non-negative.")

        def import_dm_by_name(name):
            try:
                full_name = "mongo_connector.doc_managers.%s" % name
                # importlib doesn't exist in 2.6, but __import__ is everywhere
                module = __import__(full_name, fromlist=(name,))
                dm_impl = module.DocManager
                if not issubclass(dm_impl, DocManagerBase):
                    raise TypeError("DocManager must inherit DocManagerBase.")
                return module
            except ImportError:
                raise errors.InvalidConfiguration(
                    "Could not import %s." % full_name)
                sys.exit(1)
            except (AttributeError, TypeError):
                raise errors.InvalidConfiguration(
                    "No definition for DocManager found in %s." % full_name)
                sys.exit(1)

        # instantiate the doc manager objects
        dm_instances = []
        for dm in option.value:
            module = import_dm_by_name(dm['docManager'])
            kwargs = {
                'unique_key': dm['uniqueKey'],
                'auto_commit_interval': dm['autoCommitInterval']
            }
            for k in dm['args']:
                if k not in kwargs:
                    kwargs[k] = dm['args'][k]

            target_url = dm['targetURL']
            if target_url:
                dm_instances.append(module.DocManager(target_url, **kwargs))
            else:
                dm_instances.append(module.DocManager(**kwargs))

        option.value = dm_instances

    doc_managers = add_option(
        config_key="docManagers",
        default=None,
        type=list,
        apply_function=apply_doc_managers)

    # -d is to specify the doc manager file.
    doc_managers.add_cli(
        "-d", "--doc-manager", dest="doc_manager", help=
        "Used to specify the path to each doc manager "
        "file that will be used. DocManagers should be "
        "specified in the same order as their respective "
        "target addresses in the --target-urls option. "
        "URLs are assigned to doc managers "
        "respectively. Additional doc managers are "
        "implied to have no target URL. Additional URLs "
        "are implied to have the same doc manager type as "
        "the last doc manager for which a URL was "
        "specified. By default, Mongo Connector will use "
        "'doc_manager_simulator.py'.  It is recommended "
        "that all doc manager files be kept in the "
        "doc_managers folder in mongo-connector. For "
        "more information about making your own doc "
        "manager, see 'Writing Your Own DocManager' "
        "section of the wiki")

    # -d is to specify the doc manager file.
    doc_managers.add_cli(
        "-t", "--target-url",
        dest="target_url", help=
        "Specify the URL to each target system being "
        "used. For example, if you were using Solr out of "
        "the box, you could use '-t "
        "http://localhost:8080/solr' with the "
        "SolrDocManager to establish a proper connection. "
        "URLs should be specified in the same order as "
        "their respective doc managers in the "
        "--doc-managers option.  URLs are assigned to doc "
        "managers respectively. Additional doc managers "
        "are implied to have no target URL. Additional "
        "URLs are implied to have the same doc manager "
        "type as the last doc manager for which a URL was "
        "specified. "
        "Don't use quotes around addresses. ")

    # -u is to specify the mongoDB field that will serve as the unique key
    # for the target system,
    doc_managers.add_cli(
        "-u", "--unique-key", dest="unique_key", help=
        "The name of the MongoDB field that will serve "
        "as the unique key for the target system. "
        "Note that this option does not apply "
        "when targeting another MongoDB cluster. "
        "Defaults to \"_id\".")

    # --auto-commit-interval to specify auto commit time interval
    doc_managers.add_cli(
        "--auto-commit-interval", type="int",
        dest="auto_commit_interval", help=
        "Seconds in-between calls for the Doc Manager"
        " to commit changes to the target system. A value of"
        " 0 means to commit after every write operation."
        " When left unset, Mongo Connector will not make"
        " explicit commits. Some systems have"
        " their own mechanism for adjusting a commit"
        " interval, which should be preferred to this"
        " option.")

    continue_on_error = add_option(
        config_key="continueOnError",
        default=False,
        type=bool)

    def apply_ssl(option, cli_values):
        option.value = option.value or {}
        ssl_certfile = cli_values.pop('ssl_certfile')
        ssl_keyfile = cli_values.pop('ssl_keyfile')
        ssl_cert_reqs = cli_values.pop('ssl_cert_reqs')
        ssl_ca_certs = (
            cli_values.pop('ssl_ca_certs') or option.value.get('sslCACerts'))

        if ssl_cert_reqs and ssl_cert_reqs != 'ignored' and not ssl_ca_certs:
            raise errors.InvalidConfiguration(
                '--ssl-ca-certs must be provided if the '
                '--ssl-certificate-policy is not "ignored".')
        option.value.setdefault('sslCertfile', ssl_certfile)
        option.value.setdefault('sslCACerts', ssl_ca_certs)
        option.value.setdefault('sslKeyfile', ssl_keyfile)
        option.value['sslCertificatePolicy'] = _SSL_POLICY_MAP.get(
            ssl_cert_reqs)
    ssl = add_option(
        config_key="ssl",
        default=None,
        type=dict,
        apply_function=apply_ssl)
    ssl.add_cli(
        '--ssl-certfile', dest='ssl_certfile',
        help=('Path to a certificate identifying the local connection '
              'to MongoDB.')
    )
    ssl.add_cli(
        '--ssl-keyfile', dest='ssl_keyfile',
        help=('Path to the private key for --ssl-certfile. '
              'Not necessary if already included in --ssl-certfile.')
    )
    ssl.add_cli(
        '--ssl-certificate-policy', dest='ssl_cert_reqs',
        choices=('required', 'optional', 'ignored'),
        help=('Policy for validating SSL certificates provided from the other '
              'end of the connection. There are three possible values: '
              'required = Require and validate the remote certificate. '
              'optional = Validate the remote certificate only if one '
              'is provided. '
              'ignored = Remote SSL certificates are ignored completely.')
    )
    ssl.add_cli(
        '--ssl-ca-certs', dest='ssl_ca_certs',
        help=('Path to a concatenated set of certificate authority '
              'certificates to validate the other side of the connection. ')
    )

    # --continue-on-error to continue to upsert documents during a collection
    # dump, even if the documents cannot be inserted for some reason
    continue_on_error.add_cli(
        "--continue-on-error", action="store_true",
        dest="continue_on_error", help=
        "By default, if any document fails to upsert"
        " during a collection dump, the entire operation fails."
        " When this flag is enabled, normally fatal errors"
        " will be caught and logged, allowing the collection"
        " dump to continue.\n"
        "Note: Applying oplog operations to an incomplete"
        " set of documents due to errors may cause undefined"
        " behavior. Use this flag to dump only.")

    config_file = add_option()
    config_file.add_cli(
        "-c", "--config-file", dest="config_file", help=
        "Specify a JSON file to load configurations from. You can find"
        " an example config file at mongo-connector/config.json")

    tz_aware = add_option(
        config_key="timezoneAware", default=False, type=bool)
    tz_aware.add_cli(
        "--tz-aware", dest="tz_aware", action="store_true",
        help="Make all dates and times timezone-aware.")

    return result


@log_fatal_exceptions
def main():
    """ Starts the mongo connector (assuming CLI)
    """
    conf = config.Config(get_config_options())
    conf.parse_args()

    root_logger = logging.getLogger()
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s")

    loglevel = logging.INFO
    if conf['verbosity'] > 0:
        loglevel = logging.DEBUG
    root_logger.setLevel(loglevel)

    if conf['logging.type'] == 'file':
        log_out = logging.handlers.TimedRotatingFileHandler(
            conf['logging.filename'],
            when=conf['logging.rotationWhen'],
            interval=conf['logging.rotationInterval'],
            backupCount=conf['logging.rotationBackups']
        )
        print("Logging to %s." % conf['logging.filename'])

    elif conf['logging.type'] == 'syslog':
        syslog_info = conf['logging.host']
        if ':' in syslog_info:
            log_host, log_port = syslog_info.split(':')
            syslog_info = (log_host, int(log_port))
        log_out = logging.handlers.SysLogHandler(
            address=syslog_info,
            facility=conf['logging.facility']
        )
        print("Logging to system log at %s" % conf['logging.host'])

    elif conf['logging.type'] is None:
        log_out = logging.StreamHandler()

    log_out.setLevel(loglevel)
    log_out.setFormatter(formatter)
    root_logger.addHandler(log_out)

    LOG.info('Beginning Mongo Connector')

    connector = Connector.from_config(conf)
    connector.start()

    while True:
        try:
            time.sleep(3)
            if not connector.is_alive():
                break
        except KeyboardInterrupt:
            LOG.info("Caught keyboard interrupt, exiting!")
            connector.join()
            break

if __name__ == '__main__':
    main()
