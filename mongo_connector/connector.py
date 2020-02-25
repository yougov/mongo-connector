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

"""Discovers the MongoDB cluster and starts the connector.
"""

import copy
import json
import logging
import logging.handlers
import os
import platform
import pymongo
import re
import shutil
import signal
import ssl
import sys
import threading
import time

from pymongo import MongoClient

from mongo_connector import config, constants, errors, util
from mongo_connector.constants import __version__
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.command_helper import CommandHelper
from mongo_connector.util import log_fatal_exceptions, retry_until_ok
from mongo_connector.namespace_config import NamespaceConfig, validate_namespace_options
from mongo_connector.version import Version


# Monkey patch logging to add Logger.always
ALWAYS = logging.CRITICAL + 10
logging.addLevelName(ALWAYS, "ALWAYS")


def always(self, message, *args, **kwargs):
    self.log(ALWAYS, message, *args, **kwargs)


logging.Logger.always = always


LOG = logging.getLogger(__name__)

_SSL_POLICY_MAP = {
    "ignored": ssl.CERT_NONE,
    "optional": ssl.CERT_OPTIONAL,
    "required": ssl.CERT_REQUIRED,
}

_mininum_mongodb_version = None
"""The minimum MongoDB version in the source cluster."""


def get_mininum_mongodb_version():
    return _mininum_mongodb_version


def update_mininum_mongodb_version(version):
    global _mininum_mongodb_version
    if version is None:
        _mininum_mongodb_version = version
    if _mininum_mongodb_version is None or version < _mininum_mongodb_version:
        _mininum_mongodb_version = version


class Connector(threading.Thread):
    """Thread that monitors a replica set or sharded cluster.

    Creates, runs, and monitors an OplogThread for each replica set found.
    """

    def __init__(self, mongo_address, doc_managers=None, **kwargs):
        super(Connector, self).__init__()

        # can_run is set to false when we join the thread
        self.can_run = True

        # The signal that caused the connector to stop or None
        self.signal = None

        # main address - either mongos for sharded setups or a primary otherwise
        self.address = mongo_address

        # connection to the main address
        self.main_conn = None

        # List of DocManager instances
        if doc_managers:
            self.doc_managers = doc_managers
        else:
            LOG.warning("No doc managers specified, using simulator.")
            # Avoid circular import on get_mininum_mongodb_version.
            from mongo_connector.doc_managers import doc_manager_simulator

            self.doc_managers = (doc_manager_simulator.DocManager(),)

        # Password for authentication
        self.auth_key = kwargs.pop("auth_key", None)

        # Username for authentication
        self.auth_username = kwargs.pop("auth_username", None)

        # The name of the file that stores the progress of the OplogThreads
        self.oplog_checkpoint = kwargs.pop("oplog_checkpoint", "oplog.timestamp")

        # The set of OplogThreads created
        self.shard_set = {}

        # Dict of OplogThread/timestamp pairs to record progress
        self.oplog_progress = LockingDict()

        # Timezone awareness
        self.tz_aware = kwargs.get("tz_aware", False)

        # SSL keyword arguments to MongoClient.
        ssl_certfile = kwargs.pop("ssl_certfile", None)
        ssl_ca_certs = kwargs.pop("ssl_ca_certs", None)
        ssl_keyfile = kwargs.pop("ssl_keyfile", None)
        ssl_cert_reqs = kwargs.pop("ssl_cert_reqs", None)
        self.ssl_kwargs = {}
        if ssl_certfile is not None:
            self.ssl_kwargs["ssl_certfile"] = ssl_certfile
        if ssl_ca_certs is not None:
            self.ssl_kwargs["ssl_ca_certs"] = ssl_ca_certs
        if ssl_keyfile is not None:
            self.ssl_kwargs["ssl_keyfile"] = ssl_keyfile
        if ssl_cert_reqs is not None:
            self.ssl_kwargs["ssl_cert_reqs"] = ssl_cert_reqs

        # Save the rest of kwargs.
        self.kwargs = kwargs

        # The namespace configuration shared by all OplogThreads and
        # DocManagers
        self.namespace_config = NamespaceConfig(
            namespace_set=kwargs.get("ns_set"),
            ex_namespace_set=kwargs.get("ex_ns_set"),
            gridfs_set=kwargs.get("gridfs_set"),
            dest_mapping=kwargs.get("dest_mapping"),
            namespace_options=kwargs.get("namespace_options"),
            include_fields=kwargs.get("fields"),
            exclude_fields=kwargs.get("exclude_fields"),
        )

        # Initialize and set the command helper
        command_helper = CommandHelper(self.namespace_config)
        for dm in self.doc_managers:
            dm.command_helper = command_helper

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                info_str = (
                    "MongoConnector: Can't find %s, "
                    "attempting to create an empty progress log" % self.oplog_checkpoint
                )
                LOG.warning(info_str)
                try:
                    # Create oplog progress file
                    open(self.oplog_checkpoint, "w").close()
                except IOError as e:
                    LOG.critical(
                        "MongoConnector: Could not "
                        "create a progress log: %s" % str(e)
                    )
                    sys.exit(2)
            else:
                if not os.access(self.oplog_checkpoint, os.W_OK) and not os.access(
                    self.oplog_checkpoint, os.R_OK
                ):
                    LOG.critical(
                        "Invalid permissions on %s! Exiting" % (self.oplog_checkpoint)
                    )
                    sys.exit(2)

    @classmethod
    def from_config(cls, config):
        """Create a new Connector instance from a Config object."""
        auth_key = None
        password_file = config["authentication.passwordFile"]
        if password_file is not None:
            try:
                auth_key = open(config["authentication.passwordFile"]).read()
                auth_key = re.sub(r"\s", "", auth_key)
            except IOError:
                LOG.error("Could not load password file!")
                sys.exit(1)
        password = config["authentication.password"]
        if password is not None:
            auth_key = password
        connector = Connector(
            mongo_address=config["mainAddress"],
            doc_managers=config["docManagers"],
            oplog_checkpoint=os.path.abspath(config["oplogFile"]),
            collection_dump=config["onlyDump"] or not config["noDump"],
            only_dump=config["onlyDump"],
            batch_size=config["batchSize"],
            continue_on_error=config["continueOnError"],
            auth_username=config["authentication.adminUsername"],
            auth_key=auth_key,
            fields=config["fields"],
            exclude_fields=config["exclude_fields"],
            ns_set=config["namespaces.include"],
            ex_ns_set=config["namespaces.exclude"],
            dest_mapping=config["namespaces.mapping"],
            namespace_options=config["namespaces.namespace_options"],
            gridfs_set=config["namespaces.gridfs"],
            ssl_certfile=config["ssl.sslCertfile"],
            ssl_keyfile=config["ssl.sslKeyfile"],
            ssl_ca_certs=config["ssl.sslCACerts"],
            ssl_cert_reqs=config["ssl.sslCertificatePolicy"],
            tz_aware=config["timezoneAware"],
        )
        return connector

    def join(self):
        """ Joins thread, stops it from running
        """
        self.can_run = False
        super(Connector, self).join()
        for dm in self.doc_managers:
            dm.stop()

    def write_oplog_progress(self):
        """ Writes oplog progress to file provided by user
        """

        if self.oplog_checkpoint is None:
            return None

        with self.oplog_progress as oplog_prog:
            oplog_dict = oplog_prog.get_dict()
        items = [[name, util.bson_ts_to_long(oplog_dict[name])] for name in oplog_dict]
        if not items:
            return

        # write to temp file
        backup_file = self.oplog_checkpoint + ".backup"
        os.rename(self.oplog_checkpoint, backup_file)

        # for each of the threads write to file
        with open(self.oplog_checkpoint, "w") as dest:
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
                with open(backup_file, "r") as backup:
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

        with open(self.oplog_checkpoint, "r") as progress_file:
            try:
                data = json.load(progress_file)
            except ValueError:
                LOG.exception(
                    'Cannot read oplog progress file "%s". '
                    "It may be corrupt after Mongo Connector was shut down"
                    "uncleanly. You can try to recover from a backup file "
                    '(may be called "%s.backup") or create a new progress file '
                    "starting at the current moment in time by running "
                    "mongo-connector --no-dump <other options>. "
                    "You may also be trying to read an oplog progress file "
                    "created with the old format for sharded clusters. "
                    "See https://github.com/10gen-labs/mongo-connector/wiki"
                    "/Oplog-Progress-File for complete documentation."
                    % (self.oplog_checkpoint, self.oplog_checkpoint)
                )
                return
            # data format:
            # [name, timestamp] = replica set
            # [[name, timestamp], [name, timestamp], ...] = sharded cluster
            if not isinstance(data[0], list):
                data = [data]
            with self.oplog_progress:
                self.oplog_progress.dict = dict(
                    (name, util.long_to_bson_ts(timestamp)) for name, timestamp in data
                )

    @staticmethod
    def copy_uri_options(hosts, mongodb_uri):
        """Returns a MongoDB URI to hosts with the options from mongodb_uri.
        """
        if "?" in mongodb_uri:
            options = mongodb_uri.split("?", 1)[1]
        else:
            options = None
        uri = "mongodb://" + hosts
        if options:
            uri += "/?" + options
        return uri

    def create_authed_client(self, hosts=None, **kwargs):
        kwargs.update(self.ssl_kwargs)
        if hosts is None:
            new_uri = self.address
        else:
            new_uri = self.copy_uri_options(hosts, self.address)
        client = MongoClient(new_uri, tz_aware=self.tz_aware, **kwargs)
        if self.auth_key is not None:
            client["admin"].authenticate(self.auth_username, self.auth_key)
        return client

    def update_version_from_client(self, client):
        is_master = client.admin.command("isMaster")
        for host in is_master["hosts"]:
            update_mininum_mongodb_version(
                Version.from_client(self.create_authed_client(host))
            )

    @log_fatal_exceptions
    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        # Reset the global minimum MongoDB version
        update_mininum_mongodb_version(None)
        self.main_conn = self.create_authed_client()
        LOG.always(
            "Source MongoDB version: %s",
            self.main_conn.admin.command("buildInfo")["version"],
        )

        for dm in self.doc_managers:
            name = dm.__class__.__module__
            module = sys.modules[name]
            version = "unknown"
            if hasattr(module, "__version__"):
                version = module.__version__
            elif hasattr(module, "version"):
                version = module.version
            LOG.always("Target DocManager: %s version: %s", name, version)

        self.read_oplog_progress()
        conn_type = None

        try:
            self.main_conn.admin.command("isdbgrid")
        except pymongo.errors.OperationFailure:
            conn_type = "REPLSET"

        if conn_type == "REPLSET":
            # Make sure we are connected to a replica set
            is_master = self.main_conn.admin.command("isMaster")
            if "setName" not in is_master:
                LOG.error(
                    'No replica set at "%s"! A replica set is required '
                    "to run mongo-connector. Shutting down..." % self.address
                )
                return

            # Establish a connection to the replica set as a whole
            self.main_conn.close()
            self.main_conn = self.create_authed_client(replicaSet=is_master["setName"])

            self.update_version_from_client(self.main_conn)

            # non sharded configuration
            oplog = OplogThread(
                self.main_conn,
                self.doc_managers,
                self.oplog_progress,
                self.namespace_config,
                **self.kwargs
            )
            self.shard_set[0] = oplog
            LOG.info("MongoConnector: Starting connection thread %s" % self.main_conn)
            oplog.start()

            while self.can_run:
                shard_thread = self.shard_set[0]
                if not (shard_thread.running and shard_thread.is_alive()):
                    LOG.error(
                        "MongoConnector: OplogThread"
                        " %s unexpectedly stopped! Shutting down"
                        % (str(self.shard_set[0]))
                    )
                    self.oplog_thread_join()
                    for dm in self.doc_managers:
                        dm.stop()
                    return

                self.write_oplog_progress()
                time.sleep(1)

        else:  # sharded cluster
            while self.can_run:
                # The backup role does not provide the listShards privilege,
                # so use the config.shards collection instead.
                for shard_doc in retry_until_ok(
                    lambda: list(self.main_conn.config.shards.find())
                ):
                    shard_id = shard_doc["_id"]
                    if shard_id in self.shard_set:
                        shard_thread = self.shard_set[shard_id]
                        if not (shard_thread.running and shard_thread.is_alive()):
                            LOG.error(
                                "MongoConnector: OplogThread "
                                "%s unexpectedly stopped! Shutting "
                                "down" % (str(self.shard_set[shard_id]))
                            )
                            self.oplog_thread_join()
                            for dm in self.doc_managers:
                                dm.stop()
                            return

                        self.write_oplog_progress()
                        time.sleep(1)
                        continue
                    try:
                        repl_set, hosts = shard_doc["host"].split("/")
                    except ValueError:
                        cause = "The system only uses replica sets!"
                        LOG.exception("MongoConnector: %s", cause)
                        self.oplog_thread_join()
                        for dm in self.doc_managers:
                            dm.stop()
                        return

                    shard_conn = self.create_authed_client(hosts, replicaSet=repl_set)
                    self.update_version_from_client(shard_conn)
                    oplog = OplogThread(
                        shard_conn,
                        self.doc_managers,
                        self.oplog_progress,
                        self.namespace_config,
                        mongos_client=self.main_conn,
                        **self.kwargs
                    )
                    self.shard_set[shard_id] = oplog
                    msg = "Starting connection thread"
                    LOG.info("MongoConnector: %s %s" % (msg, shard_conn))
                    oplog.start()

        if self.signal is not None:
            LOG.info("recieved signal %s: shutting down...", self.signal)
        self.oplog_thread_join()
        self.write_oplog_progress()

    def oplog_thread_join(self):
        """Stops all the OplogThreads
        """
        LOG.info("MongoConnector: Stopping all OplogThreads")
        for thread in self.shard_set.values():
            thread.join()


def get_config_options():
    result = []

    def add_option(*args, **kwargs):
        opt = config.Option(*args, **kwargs)
        result.append(opt)
        return opt

    main_address = add_option(
        config_key="mainAddress", default="localhost:27017", type=str
    )

    # -m is for the main address, which is a host:port pair, ideally of the
    # mongos. For non sharded clusters, it can be the primary.
    main_address.add_cli(
        "-m",
        "--main",
        dest="main_address",
        help="Specify the main address, which is a"
        " host:port pair. For sharded clusters, this"
        " should be the mongos address. For individual"
        " replica sets, supply the address of the"
        " primary. For example, `-m localhost:27217`"
        " would be a valid argument to `-m`. Don't use"
        " quotes around the address.",
    )

    oplog_file = add_option(config_key="oplogFile", default="oplog.timestamp", type=str)

    # -o is to specify the oplog-config file. This file is used by the system
    # to store the last timestamp read on a specific oplog. This allows for
    # quick recovery from failure.
    oplog_file.add_cli(
        "-o",
        "--oplog-ts",
        dest="oplog_file",
        help="Specify the name of the file that stores the "
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
        "incorrectly.",
    )

    no_dump = add_option(config_key="noDump", default=False, type=bool)

    # --no-dump specifies whether we should read an entire collection from
    # scratch if no timestamp is found in the oplog_config.
    no_dump.add_cli(
        "--no-dump",
        action="store_true",
        dest="no_dump",
        help="If specified, this flag will ensure that "
        "mongo_connector won't read the entire contents of a "
        "namespace iff --oplog-ts points to an empty file.",
    )

    # --only-dump specifies whether or not we should read the oplog after
    # the initial dump is finished
    only_dump = add_option(
        config_key="onlyDump",
        default=False,
        type=bool)

    only_dump.add_cli(
        "--only-dump", action="store_true", dest="only_dump", help=
        "If specified, this flag will ensure that "
        "mongo_connector won't try to read the oplog after "
        "the namespaces have been dumped.")

    batch_size = add_option(
        config_key="batchSize", default=constants.DEFAULT_BATCH_SIZE, type=int
    )

    # --batch-size specifies num docs to read from oplog before updating the
    # --oplog-ts config file with current oplog position
    batch_size.add_cli(
        "--batch-size",
        type="int",
        dest="batch_size",
        help="Specify an int to update the --oplog-ts "
        "config file with latest position of oplog every "
        "N documents. By default, the oplog config isn't "
        "updated until we've read through the entire oplog. "
        "You may want more frequent updates if you are at risk "
        "of falling behind the earliest timestamp in the oplog",
    )

    def apply_verbosity(option, cli_values):
        if cli_values["verbose"]:
            option.value = 3
        if option.value < 0 or option.value > 3:
            raise errors.InvalidConfiguration("verbosity must be in the range [0, 3].")

    # Default is warnings and above.
    verbosity = add_option(
        config_key="verbosity", default=1, type=int, apply_function=apply_verbosity
    )

    # -v enables verbose logging
    verbosity.add_cli(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        help="Enables verbose logging.",
    )

    def apply_logging(option, cli_values):
        log_mechs_enabled = [
            cli_values[m]
            for m in ("logfile", "enable_syslog", "stdout")
            if cli_values[m]
        ]
        if len(log_mechs_enabled) > 1:
            raise errors.InvalidConfiguration(
                "You cannot specify more than one logging method "
                "simultaneously. Please choose the logging method you "
                "prefer. "
            )
        if cli_values["log_format"]:
            option.value["format"] = cli_values["log_format"]

        if cli_values["logfile"]:
            when = cli_values["logfile_when"]
            interval = cli_values["logfile_interval"]
            if (
                when
                and when.startswith("W")
                and interval != constants.DEFAULT_LOGFILE_INTERVAL
            ):
                raise errors.InvalidConfiguration(
                    "You cannot specify a log rotation interval when rotating "
                    "based on a weekday (W0 - W6)."
                )

            option.value["type"] = "file"
            option.value["filename"] = cli_values["logfile"]
            if when:
                option.value["rotationWhen"] = when
            if interval:
                option.value["rotationInterval"] = interval
            if cli_values["logfile_backups"]:
                option.value["rotationBackups"] = cli_values["logfile_backups"]

        if cli_values["enable_syslog"]:
            option.value["type"] = "syslog"

        if cli_values["syslog_host"]:
            option.value["host"] = cli_values["syslog_host"]

        if cli_values["syslog_facility"]:
            option.value["facility"] = cli_values["syslog_facility"]

        if cli_values["stdout"]:
            option.value["type"] = "stream"

        # Expand the full path to log file
        option.value["filename"] = os.path.abspath(option.value["filename"])

    default_logging = {
        "type": "file",
        "filename": "mongo-connector.log",
        "format": constants.DEFAULT_LOG_FORMAT,
        "rotationInterval": constants.DEFAULT_LOGFILE_INTERVAL,
        "rotationBackups": constants.DEFAULT_LOGFILE_BACKUPCOUNT,
        "rotationWhen": constants.DEFAULT_LOGFILE_WHEN,
        "host": constants.DEFAULT_SYSLOG_HOST,
        "facility": constants.DEFAULT_SYSLOG_FACILITY,
    }

    logging = add_option(
        config_key="logging",
        default=default_logging,
        type=dict,
        apply_function=apply_logging,
    )

    # -w enables logging to a file
    logging.add_cli(
        "-w", "--logfile", dest="logfile", help="Log all output to the specified file."
    )

    logging.add_cli(
        "--stdout",
        dest="stdout",
        action="store_true",
        help="Log all output to STDOUT rather than a logfile.",
    )

    logging.add_cli(
        "--log-format",
        dest="log_format",
        help="Define a specific format for the log file. "
        "This is based on the python logging lib. "
        "Available parameters can be found at "
        "https://docs.python.org/2/library/logging.html#logrecord-attributes",
    )

    # -s is to enable syslog logging.
    logging.add_cli(
        "-s",
        "--enable-syslog",
        action="store_true",
        dest="enable_syslog",
        help="The syslog host, which may be an address like 'localhost:514' or, "
        "on Unix/Linux, the path to a Unix domain socket such as '/dev/log'.",
    )

    # --syslog-host is to specify the syslog host.
    logging.add_cli(
        "--syslog-host",
        dest="syslog_host",
        help="Used to specify the syslog host." " The default is 'localhost:514'",
    )

    # --syslog-facility is to specify the syslog facility.
    logging.add_cli(
        "--syslog-facility",
        dest="syslog_facility",
        help="Used to specify the syslog facility." " The default is 'user'",
    )

    # --logfile-when specifies the type of interval of the rotating file
    # (seconds, minutes, hours)
    logging.add_cli(
        "--logfile-when",
        action="store",
        dest="logfile_when",
        type="string",
        help="The type of interval for rotating the log file. "
        "Should be one of "
        "'S' (seconds), 'M' (minutes), 'H' (hours), "
        "'D' (days), 'W0' - 'W6' (days of the week 0 - 6), "
        "or 'midnight' (the default). See the Python documentation "
        "for 'logging.handlers.TimedRotatingFileHandler' for more "
        "details.",
    )

    # --logfile-interval specifies when to create a new log file
    logging.add_cli(
        "--logfile-interval",
        action="store",
        dest="logfile_interval",
        type="int",
        help="How frequently to rotate the log file, "
        "specifically, how many units of the rotation interval "
        "should pass before the rotation occurs. For example, "
        "to create a new file each hour: "
        " '--logfile-when=H --logfile-interval=1'. "
        "Defaults to 1. You may not use this option if "
        "--logfile-when is set to a weekday (W0 - W6). "
        "See the Python documentation for "
        "'logging.handlers.TimedRotatingFileHandler' for more "
        "details. ",
    )

    # --logfile-backups specifies how many log files will be kept.
    logging.add_cli(
        "--logfile-backups",
        action="store",
        dest="logfile_backups",
        type="int",
        help="How many log files will be kept after rotation. "
        "If set to zero, then no log files will be deleted. "
        "Defaults to 7.",
    )

    def apply_authentication(option, cli_values):
        if cli_values["admin_username"]:
            option.value["adminUsername"] = cli_values["admin_username"]

        if cli_values["password"]:
            option.value["password"] = cli_values["password"]

        if cli_values["password_file"]:
            option.value["passwordFile"] = cli_values["password_file"]

        if option.value.get("adminUsername"):
            password = option.value.get("password")
            passwordFile = option.value.get("passwordFile")
            if not password and not passwordFile:
                raise errors.InvalidConfiguration(
                    "Admin username specified without password."
                )
            if password and passwordFile:
                raise errors.InvalidConfiguration(
                    "Can't specify both password and password file."
                )

    default_authentication = {
        "adminUsername": None,
        "password": None,
        "passwordFile": None,
    }

    authentication = add_option(
        config_key="authentication",
        default=default_authentication,
        type=dict,
        apply_function=apply_authentication,
    )

    # -a is to specify the username for authentication.
    authentication.add_cli(
        "-a",
        "--admin-username",
        dest="admin_username",
        help="Used to specify the username of an admin user to "
        "authenticate with. To use authentication, the user "
        "must specify both an admin username and a keyFile.",
    )

    # -p is to specify the password used for authentication.
    authentication.add_cli(
        "-p",
        "--password",
        dest="password",
        help="Used to specify the password."
        " This is used by mongos to authenticate"
        " connections to the shards, and in the"
        " oplog threads. If authentication is not used, then"
        " this field can be left empty as the default ",
    )

    # -f is to specify the authentication key file. This file is used by mongos
    # to authenticate connections to the shards, and we'll use it in the oplog
    # threads.
    authentication.add_cli(
        "-f",
        "--password-file",
        dest="password_file",
        help="Used to store the password for authentication."
        " Use this option if you wish to specify a"
        " username and password but don't want to"
        " type in the password. The contents of this"
        " file should be the password for the admin user.",
    )

    def apply_fields(option, cli_values):
        if cli_values["fields"]:
            option.value = cli_values["fields"].split(",")

    fields = add_option(
        config_key="fields", default=[], type=list, apply_function=apply_fields
    )

    # -i to specify the list of fields to export
    fields.add_cli(
        "-i",
        "--fields",
        dest="fields",
        help="Use a comma separated list of fields to specify multiple fields. "
        "Will copy over the fields specified into a new document."
        "The '_id', 'ns' and '_ts' fields are always "
        "exported. Supports dot notation for document fields but cannot span "
        "arrays. Cannot use both 'fields' and 'exclude_fields'.",
    )

    def apply_exclude_fields(option, cli_values):
        if cli_values["exclude_fields"]:
            option.value = cli_values["exclude_fields"].split(",")

    exclude_fields = add_option(
        config_key="exclude_fields",
        default=[],
        type=list,
        apply_function=apply_exclude_fields,
    )

    # -i to specify the list of fields to exclude
    exclude_fields.add_cli(
        "-e",
        "--exclude_fields",
        dest="exclude_fields",
        help="Use a comma separated list of fields to specify multiple "
        "fields to exclude. Will delete the fields specified from the "
        "existing document. The '_id', 'ns' and '_ts' fields are always "
        "exported. Supports dot notation for document fields but cannot span "
        "arrays. Cannot use both 'fields' and 'exclude_fields'.",
    )

    def merge_namespaces_cli(option, cli_values):
        def validate_no_duplicates(lst, list_name):
            if len(lst) != len(set(lst)):
                raise errors.InvalidConfiguration(
                    "%s should not contain any duplicates." % (list_name,)
                )

        if cli_values["ns_set"]:
            option.value["include"] = cli_values["ns_set"].split(",")

        if cli_values["ex_ns_set"]:
            option.value["exclude"] = cli_values["ex_ns_set"].split(",")

        if cli_values["gridfs_set"]:
            option.value["gridfs"] = cli_values["gridfs_set"].split(",")

        if cli_values["dest_ns_set"]:
            ns_set = option.value["include"]
            dest_ns_set = cli_values["dest_ns_set"].split(",")
            if len(ns_set) != len(dest_ns_set):
                raise errors.InvalidConfiguration(
                    "Destination namespace set should be the"
                    " same length as the origin namespace set."
                )
            option.value["mapping"] = dict(zip(ns_set, dest_ns_set))

        # Check for duplicates
        if option.value["include"]:
            validate_no_duplicates(option.value["include"], "Include namespace set")
        if option.value["exclude"]:
            validate_no_duplicates(option.value["exclude"], "Exclude namespace set")
        if option.value["gridfs"]:
            validate_no_duplicates(option.value["gridfs"], "GridFS namespace set")
        if option.value["mapping"]:
            validate_no_duplicates(
                list(option.value["mapping"].values()), "Destination namespace set"
            )

    def apply_namespaces(option, cli_values):
        if (
            option.value["include"]
            or option.value["exclude"]
            or option.value["mapping"]
            or option.value["gridfs"]
        ):
            return apply_old_namespace_options(option, cli_values)
        else:
            return apply_new_namespace_options(option, cli_values)

    def apply_new_namespace_options(option, cli_values):
        """Apply the new format (since 2.5.0) namespaces options."""
        merge_namespaces_cli(option, cli_values)
        namespace_options = copy.deepcopy(option.value)
        ns_set = namespace_options.pop("include", None)
        ex_ns_set = namespace_options.pop("exclude", None)
        gridfs_set = namespace_options.pop("gridfs", None)
        dest_mapping = namespace_options.pop("mapping", None)
        option.value["namespace_options"] = namespace_options

        validate_namespace_options(
            namespace_set=ns_set,
            ex_namespace_set=ex_ns_set,
            dest_mapping=dest_mapping,
            namespace_options=namespace_options,
            gridfs_set=gridfs_set,
        )

    def apply_old_namespace_options(option, cli_values):
        """Apply the old format (before 2.5.0) namespaces options."""
        merge_namespaces_cli(option, cli_values)

        LOG.warning(
            "Deprecation warning: the current namespaces "
            "configuration format is outdated and support may be "
            "removed in a future release. Please update your "
            "config file to use the new format."
        )

        ns_set = option.value["include"]
        ex_ns_set = option.value["exclude"]
        gridfs_set = option.value["gridfs"]
        dest_mapping = option.value["mapping"]

        valid_names = set(["include", "exclude", "gridfs", "mapping"])
        valid_names |= set("__" + name for name in valid_names)
        valid_names.add("__comment__")
        for key in option.value:
            if key not in valid_names:
                raise errors.InvalidConfiguration(
                    "Invalid option %s in old style (pre 2.5.0) namespaces "
                    "configuration. The only valid option names are: %r"
                    % (key, list(valid_names))
                )

        validate_namespace_options(
            namespace_set=ns_set,
            ex_namespace_set=ex_ns_set,
            dest_mapping=dest_mapping,
            gridfs_set=gridfs_set,
        )

    default_namespaces = {"include": [], "exclude": [], "mapping": {}, "gridfs": []}

    namespaces = add_option(
        config_key="namespaces",
        default=default_namespaces,
        type=dict,
        apply_function=apply_namespaces,
    )

    # -n is to specify the namespaces we want to consider. The default
    # considers all the namespaces
    namespaces.add_cli(
        "-n",
        "--namespace-set",
        dest="ns_set",
        help="Used to specify the namespaces we want to "
        "consider. For example, if we wished to store all "
        "documents from the test.test and alpha.foo "
        "namespaces, we could use `-n test.test,alpha.foo`. "
        "You can also use, for example, `-n test.*` to store "
        "documents from all the collections of db test. "
        "The default is to consider all the namespaces, "
        "excluding the system and config databases, and "
        'also ignoring the "system.indexes" collection in '
        "any database.",
    )

    # -x is to specify the namespaces we dont want to consider. The default
    # is empty
    namespaces.add_cli(
        "-x",
        "--exclude-namespace-set",
        dest="ex_ns_set",
        help="Used to specify the namespaces we do not want to "
        "consider. For example, if we wished to ignore all "
        "documents from the test.test and alpha.foo "
        "namespaces, we could use `-x test.test,alpha.foo`. "
        "You can also use, for example, `-x test.*` to ignore "
        "documents from all the collections of db test. "
        "The default is not to exclude any namespace.",
    )

    # -g is the destination namespace
    namespaces.add_cli(
        "-g",
        "--dest-namespace-set",
        dest="dest_ns_set",
        help="Specify a destination namespace mapping. Each "
        "namespace provided in the --namespace-set option "
        "will be mapped respectively according to this "
        "comma-separated list. These lists must have "
        "equal length. "
        "It also supports mapping using wildcard, for example, "
        "mapping db.* to db.new_*, means that if we have two "
        "collections db.a and db.b, they will be renamed to "
        "db.new_a and db.new_b respectively. "
        "The default is to perform no renaming.",
    )

    # --gridfs-set is the set of GridFS namespaces to consider
    namespaces.add_cli(
        "--gridfs-set",
        dest="gridfs_set",
        help="Used to specify the GridFS namespaces we want to "
        "consider. For example, if your metadata is stored in "
        "test.fs.files and chunks are stored in test.fs.chunks, "
        "you can use `--gridfs-set test.fs`.",
    )

    def apply_doc_managers(option, cli_values):
        if not option.value:
            if not cli_values["doc_manager"] and not cli_values["target_url"]:
                return
            option.value = [{}]

        # Command line options should override the first DocManager config.
        cli_to_config = dict(
            doc_manager="docManager",
            target_url="targetURL",
            auto_commit_interval="autoCommitInterval",
            unique_key="uniqueKey",
        )
        first_dm = option.value[0]
        for cli_name, value in cli_values.items():
            if value is not None:
                first_dm[cli_to_config[cli_name]] = value

        # validate doc managers and fill in default values
        for dm in option.value:
            if not isinstance(dm, dict):
                raise errors.InvalidConfiguration(
                    "Elements of docManagers must be a dict."
                )
            if "docManager" not in dm and "docManagerClassPath" not in dm:
                raise errors.InvalidConfiguration(
                    "Every element of docManagers"
                    " must contain 'docManager' property."
                )
            if not dm.get("targetURL"):
                dm["targetURL"] = None
            if not dm.get("uniqueKey"):
                dm["uniqueKey"] = constants.DEFAULT_UNIQUE_KEY
            if dm.get("autoCommitInterval") is None:
                dm["autoCommitInterval"] = constants.DEFAULT_COMMIT_INTERVAL
            if not dm.get("args"):
                dm["args"] = {}
            if not dm.get("bulkSize"):
                dm["bulkSize"] = constants.DEFAULT_MAX_BULK

            aci = dm["autoCommitInterval"]
            if aci is not None and aci < 0:
                raise errors.InvalidConfiguration(
                    "autoCommitInterval must be non-negative."
                )

        def import_dm_by_name(name):
            full_name = "mongo_connector.doc_managers.%s.DocManager" % name
            return import_dm_by_path(full_name)

        def import_dm_by_path(path):
            # Avoid circular import on get_mininum_mongodb_version.
            from mongo_connector.doc_managers.doc_manager_base import DocManagerBase

            try:
                # importlib doesn't exist in 2.6, but __import__ is everywhere
                package, klass = path.rsplit(".", 1)
                module = __import__(package, fromlist=(package,))
                dm_impl = getattr(module, klass)
                if not issubclass(dm_impl, DocManagerBase):
                    raise TypeError("DocManager must inherit DocManagerBase.")
                return dm_impl
            except ImportError as exc:
                raise errors.InvalidConfiguration(
                    "Could not import %s. It could be that this doc manager ha"
                    "s been moved out of this project and is maintained elsewh"
                    "ere. Make sure that you have the doc manager installed al"
                    "ongside mongo-connector. Check the README for a list of a"
                    "vailable doc managers. ImportError:\n%s" % (package, exc)
                )
            except (AttributeError, TypeError):
                raise errors.InvalidConfiguration(
                    "No definition for DocManager found in %s." % package
                )

        # instantiate the doc manager objects
        dm_instances = []
        for dm in option.value:
            if "docManagerClassPath" in dm:
                DocManager = import_dm_by_path(dm["docManagerClassPath"])
            else:
                DocManager = import_dm_by_name(dm["docManager"])
            kwargs = {
                "unique_key": dm["uniqueKey"],
                "auto_commit_interval": dm["autoCommitInterval"],
                "chunk_size": dm["bulkSize"],
            }
            for k in dm["args"]:
                if k not in kwargs:
                    kwargs[k] = dm["args"][k]

            target_url = dm["targetURL"]
            if target_url:
                dm_instances.append(DocManager(target_url, **kwargs))
            else:
                dm_instances.append(DocManager(**kwargs))

        option.value = dm_instances

    doc_managers = add_option(
        config_key="docManagers",
        default=None,
        type=list,
        apply_function=apply_doc_managers,
    )

    # -d is to specify the doc manager file.
    doc_managers.add_cli(
        "-d",
        "--doc-manager",
        dest="doc_manager",
        help="Used to specify the path to each doc manager "
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
        "section of the wiki",
    )

    # -d is to specify the doc manager file.
    doc_managers.add_cli(
        "-t",
        "--target-url",
        dest="target_url",
        help="Specify the URL to each target system being "
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
        "Don't use quotes around addresses. ",
    )

    # -u is to specify the mongoDB field that will serve as the unique key
    # for the target system,
    doc_managers.add_cli(
        "-u",
        "--unique-key",
        dest="unique_key",
        help="The name of the MongoDB field that will serve "
        "as the unique key for the target system. "
        "Note that this option does not apply "
        "when targeting another MongoDB cluster. "
        'Defaults to "_id".',
    )

    # --auto-commit-interval to specify auto commit time interval
    doc_managers.add_cli(
        "--auto-commit-interval",
        type="int",
        dest="auto_commit_interval",
        help="Seconds in-between calls for the Doc Manager"
        " to commit changes to the target system. A value of"
        " 0 means to commit after every write operation."
        " When left unset, Mongo Connector will not make"
        " explicit commits. Some systems have"
        " their own mechanism for adjusting a commit"
        " interval, which should be preferred to this"
        " option.",
    )

    continue_on_error = add_option(
        config_key="continueOnError", default=False, type=bool
    )

    def apply_ssl(option, cli_values):
        option.value = option.value or {}
        ssl_certfile = cli_values.get("ssl_certfile")
        if ssl_certfile is None:
            ssl_certfile = option.value.get("sslCertfile")
        ssl_keyfile = cli_values.get("ssl_keyfile")
        if ssl_keyfile is None:
            ssl_keyfile = option.value.get("sslKeyfile")
        ssl_ca_certs = cli_values.get("ssl_ca_certs")
        if ssl_ca_certs is None:
            ssl_ca_certs = option.value.get("sslCACerts")
        ssl_cert_reqs = cli_values.get("ssl_cert_reqs")
        if ssl_cert_reqs is None:
            ssl_cert_reqs = option.value.get("sslCertificatePolicy")

        if ssl_cert_reqs is not None:
            if ssl_cert_reqs not in _SSL_POLICY_MAP:
                raise errors.InvalidConfiguration(
                    "sslCertificatePolicy (--ssl-certificate-policy) must be "
                    'one of %s, got "%s"' % (_SSL_POLICY_MAP.keys(), ssl_cert_reqs)
                )
            if (
                pymongo.version_tuple < (3, 0)
                and ssl_cert_reqs != "ignored"
                and not ssl_ca_certs
            ):
                raise errors.InvalidConfiguration(
                    '--ssl-certificate-policy is not "ignored" and '
                    "--ssl-ca-certs was not be provided. Either upgrade "
                    "PyMongo to >= 3.0 to load system provided CA "
                    "certificates or specify a CA file with --ssl-ca-certs."
                )

        option.value["sslCertfile"] = ssl_certfile
        option.value["sslCACerts"] = ssl_ca_certs
        option.value["sslKeyfile"] = ssl_keyfile
        option.value["sslCertificatePolicy"] = _SSL_POLICY_MAP.get(ssl_cert_reqs)

    ssl = add_option(config_key="ssl", default={}, type=dict, apply_function=apply_ssl)
    ssl.add_cli(
        "--ssl-certfile",
        dest="ssl_certfile",
        help=("Path to a certificate identifying the local connection " "to MongoDB."),
    )
    ssl.add_cli(
        "--ssl-keyfile",
        dest="ssl_keyfile",
        help=(
            "Path to the private key for --ssl-certfile. "
            "Not necessary if already included in --ssl-certfile."
        ),
    )
    ssl.add_cli(
        "--ssl-certificate-policy",
        dest="ssl_cert_reqs",
        choices=("required", "optional", "ignored"),
        help=(
            "Policy for validating SSL certificates provided from the other "
            "end of the connection. There are three possible values: "
            "required = Require and validate the remote certificate. "
            'optional = The same as "required", unless the server was '
            "configured to use anonymous ciphers. "
            "ignored = Remote SSL certificates are ignored completely."
        ),
    )
    ssl.add_cli(
        "--ssl-ca-certs",
        dest="ssl_ca_certs",
        help=(
            "Path to a concatenated set of certificate authority "
            "certificates to validate the other side of the connection. "
        ),
    )

    # --continue-on-error to continue to upsert documents during a collection
    # dump, even if the documents cannot be inserted for some reason
    continue_on_error.add_cli(
        "--continue-on-error",
        action="store_true",
        dest="continue_on_error",
        help="By default, if any document fails to upsert"
        " during a collection dump, the entire operation fails."
        " When this flag is enabled, normally fatal errors"
        " will be caught and logged, allowing the collection"
        " dump to continue.\n"
        "Note: Applying oplog operations to an incomplete"
        " set of documents due to errors may cause undefined"
        " behavior. Use this flag to dump only.",
    )

    config_file = add_option()
    config_file.add_cli(
        "-c",
        "--config-file",
        dest="config_file",
        help="Specify a JSON file to load configurations from. You can find"
        " an example config file at mongo-connector/config.json",
    )

    tz_aware = add_option(config_key="timezoneAware", default=False, type=bool)
    tz_aware.add_cli(
        "--tz-aware",
        dest="tz_aware",
        action="store_true",
        help="Make all dates and times timezone-aware.",
    )

    return result


def setup_logging(conf):
    root_logger = logging.getLogger()
    formatter = logging.Formatter(conf["logging.format"])

    log_levels = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
    loglevel = log_levels[conf["verbosity"]]
    root_logger.setLevel(loglevel)

    if conf["logging.type"] == "file":
        log_out = logging.handlers.TimedRotatingFileHandler(
            conf["logging.filename"],
            when=conf["logging.rotationWhen"],
            interval=conf["logging.rotationInterval"],
            backupCount=conf["logging.rotationBackups"],
        )
        print("Logging to %s." % conf["logging.filename"])
    elif conf["logging.type"] == "syslog":
        syslog_info = conf["logging.host"]
        if ":" in syslog_info:
            log_host, log_port = syslog_info.split(":")
            syslog_info = (log_host, int(log_port))
        log_out = logging.handlers.SysLogHandler(
            address=syslog_info, facility=conf["logging.facility"]
        )
        print("Logging to system log at %s" % conf["logging.host"])
    elif conf["logging.type"] == "stream":
        log_out = logging.StreamHandler()
    else:
        print(
            "Logging type must be one of 'stream', 'syslog', or 'file', not "
            "'%s'." % conf["logging.type"]
        )
        sys.exit(1)

    log_out.setLevel(loglevel)
    log_out.setFormatter(formatter)
    root_logger.addHandler(log_out)
    return root_logger


def log_startup_info():
    """Log info about the current environment."""
    LOG.always("Starting mongo-connector version: %s", __version__)
    if "dev" in __version__:
        LOG.warning(
            "This is a development version (%s) of mongo-connector", __version__
        )
    LOG.always("Python version: %s", sys.version)
    LOG.always("Platform: %s", platform.platform())
    if hasattr(pymongo, "__version__"):
        pymongo_version = pymongo.__version__
    else:
        pymongo_version = pymongo.version
    LOG.always("pymongo version: %s", pymongo_version)
    if not pymongo.has_c():
        LOG.warning(
            "pymongo version %s was installed without the C extensions. "
            '"InvalidBSON: Date value out of range" errors may occur if '
            "there are documents with BSON Datetimes that represent times "
            "outside of Python's datetime limit.",
            pymongo.__version__,
        )


@log_fatal_exceptions
def main():
    """ Starts the mongo connector (assuming CLI)
    """
    # Setup an initial logging handler that buffers log messages before
    # applying the final logging configuration.
    initial_handler = logging.handlers.MemoryHandler(100)
    root_logger = logging.getLogger()
    root_logger.addHandler(initial_handler)

    # Parse configuration and setup logging.
    conf = config.Config(get_config_options())
    conf.parse_args()
    setup_logging(conf)

    # Flush the buffered log messages to the final logging handler.
    initial_handler.setTarget(root_logger.handlers[-1])
    initial_handler.flush()
    root_logger.removeHandler(initial_handler)

    log_startup_info()

    connector = Connector.from_config(conf)

    # Catch SIGTERM and SIGINT to cleanup the connector gracefully
    def signame_handler(signal_name):
        def sig_handler(signum, frame):
            # Save the signal so it can be printed later
            connector.signal = (signal_name, signum)
            connector.can_run = False

        return sig_handler

    signal.signal(signal.SIGTERM, signame_handler("SIGTERM"))
    signal.signal(signal.SIGINT, signame_handler("SIGINT"))

    connector.start()

    while True:
        if not connector.is_alive():
            break
        time.sleep(3)


if __name__ == "__main__":
    main()
