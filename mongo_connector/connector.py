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
import optparse
import os
import pymongo
import re
import shutil
import sys
import threading
import time
import imp
from mongo_connector import constants, errors, util
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.doc_managers import doc_manager_simulator as simulator

from pymongo import MongoClient


class Connector(threading.Thread):
    """Checks the cluster for shards to tail.
    """
    def __init__(self, address, oplog_checkpoint, target_url, ns_set,
                 u_key, auth_key, doc_manager=None, auth_username=None,
                 collection_dump=True, batch_size=constants.DEFAULT_BATCH_SIZE,
                 fields=None, dest_mapping={},
                 auto_commit_interval=constants.DEFAULT_COMMIT_INTERVAL):

        if target_url and not doc_manager:
            raise errors.ConnectorError("Cannot create a Connector with a "
                                        "target URL but no doc manager!")

        def is_string(s):
            try:
                return isinstance(s, basestring)
            except NameError:
                return isinstance(s, str)

        def load_doc_manager(path):
            name, _ = os.path.splitext(os.path.basename(path))
            try:
                import importlib.machinery
                loader = importlib.machinery.SourceFileLoader(name, path)
                module = loader.load_module(name)
            except ImportError:
                module = imp.load_source(name, path)
            return module

        doc_manager_modules = None

        if doc_manager is not None:
            # backwards compatilibity: doc_manager may be a string
            if is_string(doc_manager):
                doc_manager_modules = [load_doc_manager(doc_manager)]
            # doc_manager is a list
            else:
                doc_manager_modules = []
                for dm in doc_manager:
                    doc_manager_modules.append(load_doc_manager(dm))

        super(Connector, self).__init__()

        #can_run is set to false when we join the thread
        self.can_run = True

        #The name of the file that stores the progress of the OplogThreads
        self.oplog_checkpoint = oplog_checkpoint

        #main address - either mongos for sharded setups or a primary otherwise
        self.address = address

        #The URLs of each target system, respectively
        if is_string(target_url):
            self.target_urls = [target_url]
        elif target_url:
            self.target_urls = list(target_url)
        else:
            self.target_urls = None

        #The set of relevant namespaces to consider
        self.ns_set = ns_set

        #The dict of source namespace to destination namespace
        self.dest_mapping = dest_mapping

        #The key that is a unique document identifier for the target system.
        #Not necessarily the mongo unique key.
        self.u_key = u_key

        #Password for authentication
        self.auth_key = auth_key

        #Username for authentication
        self.auth_username = auth_username

        #The set of OplogThreads created
        self.shard_set = {}

        #Boolean chooses whether to dump the entire collection if no timestamp
        # is present in the config file
        self.collection_dump = collection_dump

        #Num entries to process before updating config file with current pos
        self.batch_size = batch_size

        #Dict of OplogThread/timestamp pairs to record progress
        self.oplog_progress = LockingDict()

        # List of fields to export
        self.fields = fields

        try:
            docman_kwargs = {"unique_key": u_key,
                             "namespace_set": ns_set,
                             "auto_commit_interval": auto_commit_interval}

            # No doc managers specified, using simulator
            if doc_manager is None:
                self.doc_managers = [simulator.DocManager(**docman_kwargs)]
            else:
                self.doc_managers = []
                for i, d in enumerate(doc_manager_modules):
                    # self.target_urls may be shorter than
                    # self.doc_managers, or left as None
                    if self.target_urls and i < len(self.target_urls):
                        target_url = self.target_urls[i]
                    else:
                        target_url = None

                    if target_url:
                        self.doc_managers.append(
                            d.DocManager(self.target_urls[i],
                                         **docman_kwargs))
                    else:
                        self.doc_managers.append(
                            d.DocManager(**docman_kwargs))
                # If more target URLs were given than doc managers, may need
                # to create additional doc managers
                for url in self.target_urls[i + 1:]:
                    self.doc_managers.append(
                        doc_manager_modules[-1].DocManager(url,
                                                           **docman_kwargs))
        except errors.ConnectionFailed:
            err_msg = "MongoConnector: Could not connect to target system"
            logging.critical(err_msg)
            self.can_run = False
            return

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                info_str = ("MongoConnector: Can't find %s, "
                            "attempting to create an empty progress log" %
                            self.oplog_checkpoint)
                logging.info(info_str)
                try:
                    # Create oplog progress file
                    open(self.oplog_checkpoint, "w").close()
                except IOError as e:
                    logging.critical("MongoConnector: Could not "
                                     "create a progress log: %s" %
                                     str(e))
                    sys.exit(1)
            else:
                if (not os.access(self.oplog_checkpoint, os.W_OK)
                        and not os.access(self.oplog_checkpoint, os.R_OK)):
                    logging.critical("Invalid permissions on %s! Exiting" %
                                     (self.oplog_checkpoint))
                    sys.exit(1)

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

        # write to temp file
        backup_file = self.oplog_checkpoint + '.backup'
        os.rename(self.oplog_checkpoint, backup_file)

        # for each of the threads write to file
        with open(self.oplog_checkpoint, 'w') as dest:
            with self.oplog_progress as oplog_prog:

                oplog_dict = oplog_prog.get_dict()
                for oplog, time_stamp in oplog_dict.items():
                    oplog_str = str(oplog)
                    timestamp = util.bson_ts_to_long(time_stamp)
                    json_str = json.dumps([oplog_str, timestamp])
                    try:
                        dest.write(json_str)
                    except IOError:
                        # Basically wipe the file, copy from backup
                        dest.truncate()
                        with open(backup_file, 'r') as backup:
                            shutil.copyfile(backup, dest)
                        break

        os.remove(self.oplog_checkpoint + '.backup')

    def read_oplog_progress(self):
        """Reads oplog progress from file provided by user.
        This method is only called once before any threads are spanwed.
        """

        if self.oplog_checkpoint is None:
            return None

        # Check for empty file
        try:
            if os.stat(self.oplog_checkpoint).st_size == 0:
                logging.info("MongoConnector: Empty oplog progress file.")
                return None
        except OSError:
            return None

        source = open(self.oplog_checkpoint, 'r')
        try:
            data = json.load(source)
        except ValueError:       # empty file
            reason = "It may be empty or corrupt."
            logging.info("MongoConnector: Can't read oplog progress file. %s" %
                         (reason))
            source.close()
            return None

        source.close()

        count = 0
        oplog_dict = self.oplog_progress.get_dict()
        for count in range(0, len(data), 2):
            oplog_str = data[count]
            time_stamp = data[count + 1]
            oplog_dict[oplog_str] = util.long_to_bson_ts(time_stamp)
            #stored as bson_ts

    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        main_conn = MongoClient(self.address)
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
            if not "setName" in is_master:
                logging.error(
                    'No replica set at "%s"! A replica set is required '
                    'to run mongo-connector. Shutting down...' % self.address
                )
                return

            # Establish a connection to the replica set as a whole
            main_conn.disconnect()
            main_conn = MongoClient(self.address,
                                    replicaSet=is_master['setName'])
            if self.auth_key is not None:
                main_conn.admin.authenticate(self.auth_username, self.auth_key)

            #non sharded configuration
            oplog_coll = main_conn['local']['oplog.rs']

            prim_admin = main_conn.admin
            repl_set = prim_admin.command("replSetGetStatus")['set']

            oplog = OplogThread(
                primary_conn=main_conn,
                main_address=(main_conn.host + ":" + str(main_conn.port)),
                oplog_coll=oplog_coll,
                is_sharded=False,
                doc_manager=self.doc_managers,
                oplog_progress_dict=self.oplog_progress,
                namespace_set=self.ns_set,
                auth_key=self.auth_key,
                auth_username=self.auth_username,
                repl_set=repl_set,
                collection_dump=self.collection_dump,
                batch_size=self.batch_size,
                fields=self.fields,
                dest_mapping=self.dest_mapping
            )
            self.shard_set[0] = oplog
            logging.info('MongoConnector: Starting connection thread %s' %
                         main_conn)
            oplog.start()

            while self.can_run:
                if not self.shard_set[0].running:
                    logging.error("MongoConnector: OplogThread"
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
                            logging.error("MongoConnector: OplogThread "
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
                        logging.error("MongoConnector: %s", cause)
                        self.oplog_thread_join()
                        for dm in self.doc_managers:
                            dm.stop()
                        return

                    shard_conn = MongoClient(hosts, replicaSet=repl_set)
                    oplog_coll = shard_conn['local']['oplog.rs']

                    oplog = OplogThread(
                        primary_conn=shard_conn,
                        main_address=self.address,
                        oplog_coll=oplog_coll,
                        is_sharded=True,
                        doc_manager=self.doc_managers,
                        oplog_progress_dict=self.oplog_progress,
                        namespace_set=self.ns_set,
                        auth_key=self.auth_key,
                        auth_username=self.auth_username,
                        collection_dump=self.collection_dump,
                        batch_size=self.batch_size,
                        fields=self.fields,
                        dest_mapping=self.dest_mapping
                    )
                    self.shard_set[shard_id] = oplog
                    msg = "Starting connection thread"
                    logging.info("MongoConnector: %s %s" % (msg, shard_conn))
                    oplog.start()

        self.oplog_thread_join()
        self.write_oplog_progress()

    def oplog_thread_join(self):
        """Stops all the OplogThreads
        """
        logging.info('MongoConnector: Stopping all OplogThreads')
        for thread in self.shard_set.values():
            thread.join()


def main():
    """ Starts the mongo connector (assuming CLI)
    """
    parser = optparse.OptionParser()

    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    parser.add_option("-m", "--main", action="store", type="string",
                      dest="main_addr", default="localhost:27217",
                      help="""Specify the main address, which is a"""
                      """ host:port pair. For sharded clusters, this"""
                      """ should be the mongos address. For individual"""
                      """ replica sets, supply the address of the"""
                      """ primary. For example, `-m localhost:27217`"""
                      """ would be a valid argument to `-m`. Don't use"""
                      """ quotes around the address.""")

    #-o is to specify the oplog-config file. This file is used by the system
    #to store the last timestamp read on a specific oplog. This allows for
    #quick recovery from failure.
    parser.add_option("-o", "--oplog-ts", action="store", type="string",
                      dest="oplog_config", default="config.txt",
                      help="""Specify the name of the file that stores the """
                      """oplog progress timestamps. """
                      """This file is used by the system to store the last """
                      """timestamp read on a specific oplog. This allows """
                      """for quick recovery from failure. By default this """
                      """is `config.txt`, which starts off empty. An empty """
                      """file causes the system to go through all the mongo """
                      """oplog and sync all the documents. Whenever the """
                      """cluster is restarted, it is essential that the """
                      """oplog-timestamp config file be emptied - otherwise """
                      """the connector will miss some documents and behave """
                      """incorrectly.""")

    #--no-dump specifies whether we should read an entire collection from
    #scratch if no timestamp is found in the oplog_config.
    parser.add_option("--no-dump", action="store_true", default=False, help=
                      "If specified, this flag will ensure that "
                      "mongo_connector won't read the entire contents of a "
                      "namespace iff --oplog-ts points to an empty file.")

    #--batch-size specifies num docs to read from oplog before updating the
    #--oplog-ts config file with current oplog position
    parser.add_option("--batch-size", action="store",
                      default=constants.DEFAULT_BATCH_SIZE, type="int",
                      help="Specify an int to update the --oplog-ts "
                      "config file with latest position of oplog every "
                      "N documents. By default, the oplog config isn't "
                      "updated until we've read through the entire oplog. "
                      "You may want more frequent updates if you are at risk "
                      "of falling behind the earliest timestamp in the oplog")

    #-t is to specify the URL to the target system being used.
    parser.add_option("-t", "--target-url", "--target-urls", action="store",
                      type="string", dest="urls", default=None, help=
                      """Specify the URL to each target system being """
                      """used. For example, if you were using Solr out of """
                      """the box, you could use '-t """
                      """http://localhost:8080/solr' with the """
                      """SolrDocManager to establish a proper connection. """
                      """URLs should be specified in the same order as """
                      """their respective doc managers in the """
                      """--doc-managers option.  URLs are assigned to doc """
                      """managers respectively. Additional doc managers """
                      """are implied to have no target URL. Additional """
                      """URLs are implied to have the same doc manager """
                      """type as the last doc manager for which a URL was """
                      """specified. """
                      """Don't use quotes around addresses. """)

    #-n is to specify the namespaces we want to consider. The default
    #considers all the namespaces
    parser.add_option("-n", "--namespace-set", action="store", type="string",
                      dest="ns_set", default=None, help=
                      """Used to specify the namespaces we want to """
                      """consider. For example, if we wished to store all """
                      """documents from the test.test and alpha.foo """
                      """namespaces, we could use `-n test.test,alpha.foo`. """
                      """The default is to consider all the namespaces, """
                      """excluding the system and config databases, and """
                      """also ignoring the "system.indexes" collection in """
                      """any database.""")

    #-u is to specify the mongoDB field that will serve as the unique key
    #for the target system,
    parser.add_option("-u", "--unique-key", action="store", type="string",
                      dest="u_key", default="_id", help=
                      """Used to specify the mongoDB field that will serve """
                      """as the unique key for the target system. """
                      """The default is "_id", which can be noted by """
                      """'-u _id'""")

    #-f is to specify the authentication key file. This file is used by mongos
    #to authenticate connections to the shards, and we'll use it in the oplog
    #threads.
    parser.add_option("-f", "--password-file", action="store", type="string",
                      dest="auth_file", default=None, help=
                      """Used to store the password for authentication."""
                      """ Use this option if you wish to specify a"""
                      """ username and password but don't want to"""
                      """ type in the password. The contents of this"""
                      """ file should be the password for the admin user.""")

    #-p is to specify the password used for authentication.
    parser.add_option("-p", "--password", action="store", type="string",
                      dest="password", default=None, help=
                      """Used to specify the password."""
                      """ This is used by mongos to authenticate"""
                      """ connections to the shards, and in the"""
                      """ oplog threads. If authentication is not used, then"""
                      """ this field can be left empty as the default """)

    #-a is to specify the username for authentication.
    parser.add_option("-a", "--admin-username", action="store", type="string",
                      dest="admin_name", default="__system", help=
                      """Used to specify the username of an admin user to """
                      """authenticate with. To use authentication, the user """
                      """must specify both an admin username and a keyFile. """
                      """The default username is '__system'""")

    #-d is to specify the doc manager file.
    parser.add_option("-d", "--docManager", "--doc-managers", action="store",
                      type="string", dest="doc_managers", default=None, help=
                      """Used to specify the path to each doc manager """
                      """file that will be used. DocManagers should be """
                      """specified in the same order as their respective """
                      """target addresses in the --target-urls option. """
                      """URLs are assigned to doc managers """
                      """respectively. Additional doc managers are """
                      """implied to have no target URL. Additional URLs """
                      """are implied to have the same doc manager type as """
                      """the last doc manager for which a URL was """
                      """specified. By default, Mongo Connector will use """
                      """'doc_manager_simulator.py'.  It is recommended """
                      """that all doc manager files be kept in the """
                      """doc_managers folder in mongo-connector. For """
                      """more information about making your own doc """
                      """manager, see 'Writing Your Own DocManager' """
                      """section of the wiki""")

    #-g is the destination namespace
    parser.add_option("-g", "--dest-namespace-set", action="store",
                      type="string", dest="dest_ns_set", default=None, help=
                      """Specify a destination namespace mapping. Each """
                      """namespace provided in the --namespace-set option """
                      """will be mapped respectively according to this """
                      """comma-separated list. These lists must have """
                      """equal length. The default is to use the identity """
                      """mapping. This is currently only implemented """
                      """for mongo-to-mongo connections.""")

    #-s is to enable syslog logging.
    parser.add_option("-s", "--enable-syslog", action="store_true",
                      dest="enable_syslog", default=False, help=
                      """Used to enable logging to syslog."""
                      """ Use -l to specify syslog host.""")

    #--syslog-host is to specify the syslog host.
    parser.add_option("--syslog-host", action="store", type="string",
                      dest="syslog_host", default="localhost:514", help=
                      """Used to specify the syslog host."""
                      """ The default is 'localhost:514'""")

    #--syslog-facility is to specify the syslog facility.
    parser.add_option("--syslog-facility", action="store", type="string",
                      dest="syslog_facility", default="user", help=
                      """Used to specify the syslog facility."""
                      """ The default is 'user'""")

    #-i to specify the list of fields to export
    parser.add_option("-i", "--fields", action="store", type="string",
                      dest="fields", default=None, help=
                      """Used to specify the list of fields to export. """
                      """Specify a field or fields to include in the export. """
                      """Use a comma separated list of fields to specify multiple """
                      """fields. The '_id', 'ns' and '_ts' fields are always """
                      """exported.""")

    #--auto-commit-interval to specify auto commit time interval
    parser.add_option("--auto-commit-interval", action="store",
                      dest="commit_interval", type="int",
                      default=constants.DEFAULT_COMMIT_INTERVAL,
                      help="""Seconds in-between calls for the Doc Manager"""
                      """ to commit changes to the target system. A value of"""
                      """ 0 means to commit after every write operation."""
                      """ When left unset, Mongo Connector will not make"""
                      """ explicit commits. Some systems have"""
                      """ their own mechanism for adjusting a commit"""
                      """ interval, which should be preferred to this"""
                      """ option.""")

    #-v enables vebose logging
    parser.add_option("-v", "--verbose", action="store_true",
                      dest="verbose", default=False,
                      help="Sets verbose logging to be on.")

    #-w enable logging to a file
    parser.add_option("-w", "--logfile", dest="logfile",
                      help=("Log all output to a file rather than stream to "
                            "stderr.   Omit to stream to stderr."))

    (options, args) = parser.parse_args()

    logger = logging.getLogger()
    loglevel = logging.INFO
    if options.verbose:
        loglevel = logging.DEBUG
    logger.setLevel(loglevel)

    if options.enable_syslog and options.logfile:
        print ("You cannot specify syslog and a logfile simultaneously, please"
               " choose the logging method you would prefer.")
        sys.exit(0)

    if options.enable_syslog:
        syslog_info = options.syslog_host.split(":")
        syslog_host = logging.handlers.SysLogHandler(
            address=(syslog_info[0], int(syslog_info[1])),
            facility=options.syslog_facility
        )
        syslog_host.setLevel(loglevel)
        logger.addHandler(syslog_host)
    elif options.logfile is not None:
        try:
            log_out = logging.FileHandler(options.logfile)
        except Exception as e:
            raise e
            sys.exit(0)
        log_out.setLevel(loglevel)
        log_out.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_out)
    else:
        log_out = logging.StreamHandler()
        log_out.setLevel(loglevel)
        log_out.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(log_out)

    logger.info('Beginning Mongo Connector')

    # Get DocManagers and target URLs
    # Each DocManager is assigned the respective (same-index) target URL
    # Additional DocManagers may be specified that take no target URL
    doc_managers = options.doc_managers
    doc_managers = doc_managers.split(",") if doc_managers else doc_managers
    target_urls = options.urls.split(",") if options.urls else None

    if options.doc_managers is None:
        logger.info('No doc managers specified, using simulator.')

    if options.ns_set is None:
        ns_set = []
    else:
        ns_set = options.ns_set.split(',')

    if options.dest_ns_set is None:
        dest_ns_set = ns_set
    else:
        dest_ns_set = options.dest_ns_set.split(',')

    if len(dest_ns_set) != len(ns_set):
        logger.error("Destination namespace must be the same length as the "
                     "origin namespace!")
        sys.exit(1)
    elif len(set(ns_set)) + len(set(dest_ns_set)) != 2 * len(ns_set):
        logger.error("Namespace set and destination namespace set should not "
                     "contain any duplicates!")
        sys.exit(1)
    else:
        ## Create a mapping of source ns to dest ns as a dict
        dest_mapping = dict(zip(ns_set, dest_ns_set))

    fields = options.fields
    if fields is not None:
        fields = options.fields.split(',')

    key = None
    if options.auth_file is not None:
        try:
            key = open(options.auth_file).read()
            re.sub(r'\s', '', key)
        except IOError:
            logger.error('Could not parse password authentication file!')
            sys.exit(1)

    if options.password is not None:
        key = options.password

    if key is None and options.admin_name != "__system":
        logger.error("Admin username specified without password!")
        sys.exit(1)

    if options.commit_interval is not None and options.commit_interval < 0:
        raise ValueError("--auto-commit-interval must be non-negative")

    connector = Connector(
        address=options.main_addr,
        oplog_checkpoint=options.oplog_config,
        target_url=target_urls,
        ns_set=ns_set,
        u_key=options.u_key,
        auth_key=key,
        doc_manager=doc_managers,
        auth_username=options.admin_name,
        collection_dump=(not options.no_dump),
        batch_size=options.batch_size,
        fields=fields,
        dest_mapping=dest_mapping,
        auto_commit_interval=options.commit_interval
    )
    connector.start()

    while True:
        try:
            time.sleep(3)
            if not connector.is_alive():
                break
        except KeyboardInterrupt:
            logging.info("Caught keyboard interrupt, exiting!")
            connector.join()
            break

if __name__ == '__main__':
    main()
