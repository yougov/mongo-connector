# Copyright 2012 10gen, Inc.
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

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Discovers the mongo cluster and starts the connector.
"""

import inspect
import logging
import logging.handlers
import oplog_manager
import optparse
import os
import pymongo
import re
import shutil
import subprocess
import sys
import threading
import time
import util
import imp

from locking_dict import LockingDict

try:
    import simplejson as json
except:
    import json


class Connector(threading.Thread):
    """Checks the cluster for shards to tail.
    """
    def __init__(self, address, oplog_checkpoint, target_url, ns_set,
                 u_key, auth_key, doc_manager=None, auth_username=None):
        file = inspect.getfile(inspect.currentframe())
        cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))
        if doc_manager is not None:
            doc_manager = imp.load_source('DocManager', doc_manager)
        else:
            from doc_manager import DocManager
        time.sleep(1)
        super(Connector, self).__init__()

        #can_run is set to false when we join the thread
        self.can_run = True

        #The name of the file that stores the progress of the OplogThreads
        self.oplog_checkpoint = oplog_checkpoint

        #main address - either mongos for sharded setups or a primary otherwise
        self.address = address

        #The URL of the target system
        self.target_url = target_url

        #The set of relevant namespaces to consider
        self.ns_set = ns_set

        #The key that is a unique document identifier for the target system.
        #Not necessarily the mongo unique key.
        self.u_key = u_key

        #Password for authentication
        self.auth_key = auth_key

        #Username for authentication
        self.auth_username = auth_username

        #The set of OplogThreads created
        self.shard_set = {}

        #Dict of OplogThread/timestamp pairs to record progress
        self.oplog_progress = LockingDict()

        try:
            if target_url is None:
                if doc_manager is None:  # imported using from... import
                    self.doc_manager = DocManager(unique_key=u_key)
                else:  # imported using load source
                    self.doc_manager = doc_manager.DocManager(unique_key=u_key)
            else:
                if doc_manager is None:
                    self.doc_manager = DocManager(self.target_url,
                                                  unique_key=u_key)
                else:
                    self.doc_manager = doc_manager.DocManager(self.target_url,
                                                              unique_key=u_key)
        except SystemError:
            logging.critical("MongoConnector: Bad target system URL!")
            self.can_run = False
            return

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                info_str = "MongoConnector: Can't find OplogProgress file!"
                logging.critical(info_str)
                self.doc_manager.stop()
                self.can_run = False

    def join(self):
        """ Joins thread, stops it from running
        """
        self.can_run = False
        self.doc_manager.stop()
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
                for oplog, ts in oplog_dict.items():
                    oplog_str = str(oplog)
                    timestamp = util.bson_ts_to_long(ts)
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
            err_msg = "MongoConnector: Can't read oplog progress file."
            reason = "It may be empty or corrupt."
            logging.info("%s %s" % (err_msg, reason))
            source.close()
            return None

        source.close()

        count = 0
        oplog_dict = self.oplog_progress.get_dict()
        for count in range(0, len(data), 2):
            oplog_str = data[count]
            ts = data[count + 1]
            oplog_dict[oplog_str] = util.long_to_bson_ts(ts)
            #stored as bson_ts

    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        main_conn = pymongo.Connection(self.address)
        if self.auth_key is not None:
            main_conn['admin'].authenticate(self.auth_username, self.auth_key) 
        self.read_oplog_progress()
        conn_type = None

        try:
            main_conn.admin.command("isdbgrid")
        except pymongo.errors.OperationFailure:
            conn_type = "REPLSET"

        if conn_type == "REPLSET":
            #non sharded configuration
            oplog_coll = main_conn['local']['oplog.rs']

            prim_admin = main_conn.admin
            repl_set = prim_admin.command("replSetGetStatus")['set']
            host = main_conn.host
            port = main_conn.port
            address = host + ":" + str(port)

            oplog = oplog_manager.OplogThread(main_conn, address, oplog_coll,
                                              False, self.doc_manager,
                                              self.oplog_progress,
                                              self.ns_set, self.auth_key,
                                              self.auth_username,
                                              repl_set=repl_set)
            self.shard_set[0] = oplog
            logging.info('MongoConnector: Starting connection thread %s' %
                         main_conn)
            oplog.start()

            while self.can_run:
                if not self.shard_set[0].running:
                    err_msg = "MongoConnector: OplogThread"
                    set = str(self.shard_set[0])
                    effect = "unexpectedly stopped! Shutting down."
                    logging.error("%s %s %s" % (err_msg, set, effect))
                    self.oplog_thread_join()
                    self.doc_manager.stop()
                    return

                self.write_oplog_progress()
                time.sleep(1)

        else:       # sharded cluster
            while self.can_run is True:

                shard_coll = main_conn['config']['shards']
                shard_cursor = shard_coll.find()

                for shard_doc in shard_cursor:
                    shard_id = shard_doc['_id']
                    if shard_id in self.shard_set:
                        if not self.shard_set[shard_id].running:
                            err_msg = "MongoConnector: OplogThread"
                            set = str(self.shard_set[shard_id])
                            effect = "unexpectedly stopped! Shutting down"
                            logging.error("%s %s %s" % (err_msg, set, effect))
                            self.oplog_thread_join()
                            self.doc_manager.stop()
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
                        self.doc_manager.stop()
                        return

                    shard_conn = pymongo.Connection(hosts, replicaset=repl_set)
                    oplog_coll = shard_conn['local']['oplog.rs']
                    oplog = oplog_manager.OplogThread(shard_conn, self.address,
                                                      oplog_coll, True,
                                                      self.doc_manager,
                                                      self.oplog_progress,
                                                      self.ns_set,
                                                      self.auth_key,
                                                      self.auth_username)
                    self.shard_set[shard_id] = oplog
                    msg = "Starting connection thread"
                    logging.info("MongoConnector: %s %s" % (msg, shard_conn))
                    oplog.start()

        self.oplog_thread_join()

    def oplog_thread_join(self):
        """Stops all the OplogThreads
        """
        logging.info('MongoConnector: Stopping all OplogThreads')
        for thread in self.shard_set.values():
            thread.join()

if __name__ == '__main__':
    """Runs mongo connector
    """

    parser = optparse.OptionParser()

    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    parser.add_option("-m", "--main", action="store", type="string",
                      dest="main_addr", default="localhost:27217",
                      help="""Specify the main address, which is a"""
                      """ host:port pair. For sharded clusters,  this"""
                      """ should be the mongos address. For individual"""
                      """ replica sets, supply the address of the"""
                      """ primary. For example, `-m localhost:27217`"""
                      """ would be a valid argument to `-m`. Don't use"""
                      """ quotes around the address""")

    #-o is to specify the oplog-config file. This file is used by the system
    #to store the last timestamp read on a specific oplog. This allows for
    #quick recovery from failure.
    parser.add_option("-o", "--oplog-ts", action="store", type="string",
                      dest="oplog_config", default="config.txt",
                      help="""Specify the name of the file that stores the"""
                      """oplog progress timestamps. """
                      """This file is used by the system to store the last"""
                      """timestamp read on a specific oplog. This allows"""
                      """ for quick recovery from failure. By default this"""
                      """ is `config.txt`, which starts off empty. An empty"""
                      """ file causes the system to go through all the mongo"""
                      """ oplog and sync all the documents. Whenever the """
                      """cluster is restarted, it is essential that the """
                      """oplog-timestamp config file be emptied - otherwise"""
                      """ the connector will miss some documents and behave"""
                      """incorrectly.""")

    #-t is to specify the URL to the target system being used.
    parser.add_option("-t", "--target-url", action="store", type="string",
                      dest="url", default=None,
                      help="""Specify the URL to the target system being """
                      """used. For example, if you were using Solr out of """
                      """the box, you could use '-t """
                      """ http://localhost:8080/solr' with the """
                      """ SolrDocManager to establish a proper connection."""
                      """ Don't use quotes around address."""
                      """If target system doesn't need URL, don't specify""")

    #-n is to specify the namespaces we want to consider. The default
    #considers all the namespaces
    parser.add_option("-n", "--namespace-set", action="store", type="string",
                      dest="ns_set", default=None, help=
                      """Used to specify the namespaces we want to """
                      """ consider. For example, if we wished to store all """
                      """ documents from the test.test and alpha.foo """
                      """ namespaces, we could use `-n test.test,alpha.foo`."""
                      """ The default is to consider all the namespaces, """
                      """ excluding the system and config databases, and """
                      """ also ignoring the "system.indexes" collection in """
                      """any database.""")

    #-u is to specify the mongoDB field that will serve as the unique key
    #for the target system,
    parser.add_option("-u", "--unique-key", action="store", type="string",
                      dest="u_key", default="_id", help=
                      """Used to specify the mongoDB field that will serve"""
                      """as the unique key for the target system"""
                      """The default is "_id", which can be noted by """
                      """  '-u _id'""")

    #-f is to specify the authentication key file. This file is used by mongos
    #to authenticate connections to the shards, and we'll use it in the oplog
    #threads.
    parser.add_option("-f", "--password-file", action="store", type="string",
                      dest="auth_file", default=None, help=
                      """ Used to store the password for authentication."""
                      """ Use this option if you wish to specify a"""
                      """ username and password but don't want to"""
                      """ type in the password. The contents of this"""
                      """ file should be the password for the admin user.""")

    #-p is to specify the password used for authentication.
    parser.add_option("-p", "--password", action="store", type="string",
                      dest="password", default=None, help=
                      """ Used to specify the password."""
                      """ This is used by mongos to authenticate"""
                      """ connections to the shards, and in the"""
                      """ oplog threads. If authentication is not used, then"""
                      """ this field can be left empty as the default """)

    #-a is to specify the username for authentication.
    parser.add_option("-a", "--admin-username", action="store", type="string",
                      dest="admin_name", default="__system", help=
                      """Used to specify the username of an admin user to"""
                      """authenticate with. To use authentication, the user"""
                      """must specify both an admin username and a keyFile."""
                      """The default username is '__system'""")

    #-d is to specify the doc manager file.
    parser.add_option("-d", "--docManager", action="store", type="string",
                      dest="doc_manager", default=None, help=
                      """Used to specify the doc manager file that"""
                      """ is going to be used. You should send the"""
                      """ path of the file you want to be used."""
                      """ By default, it will use the """
                      """ doc_manager_simulator.py file. It is"""
                      """ recommended that all doc manager files be"""
                      """ kept in the doc_managers folder in"""
                      """ mongo-connector. For more information"""
                      """ about making your own doc manager,"""
                      """ see Doc Manager section.""")

    #-s is to enable syslog logging.
    parser.add_option("-s","--enable-syslog", action="store_true",
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

    (options, args) = parser.parse_args()

    logger = logging.getLogger()
    loglevel = logging.INFO
    logger.setLevel(loglevel)
    
    if options.enable_syslog:
        lh = options.syslog_host.split(":")
        sh = logging.handlers.SysLogHandler(address=(lh[0], int(lh[1])),facility=options.syslog_facility)
        sh.setLevel(loglevel)
        logger.addHandler(sh)
    else:
        ch = logging.StreamHandler()
        ch.setLevel(loglevel)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    logger.info('Beginning Mongo Connector')

    if options.doc_manager is None:
        logger.info('No doc manager specified, using simulator.')
    try:
        if options.ns_set is None:
            ns_set = []
        else:
            ns_set = options.ns_set.split(',')
    except:
        logger.error('Namespaces must be separated by commas!')
        sys.exit(1)

    key = None
    if options.auth_file is not None:
        try:
            file = open(options.auth_file)
            key = file.read()
            re.sub(r'\s', '', key)
        except:
            logger.error('Could not parse password authentication file!')
            sys.exit(1)

    if options.password is not None:
        key = options.password

    if key is None and options.admin_name != "__system":
        logger.error("Admin username specified without password!")
        sys.exit(1)

    ct = Connector(options.main_addr, options.oplog_config, options.url,
                   ns_set, options.u_key, key, options.doc_manager,
                   auth_username=options.admin_name)

    ct.start()

    while True:
        try:
            time.sleep(3)
            if not ct.is_alive():
                break
        except KeyboardInterrupt:
            logging.info("Caught keyboard interrupt, exiting!")
            ct.join()
            break
