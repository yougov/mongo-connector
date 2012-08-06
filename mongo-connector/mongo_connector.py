# Copyright 2012 10gen, Inc.
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

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Discovers the mongo cluster and starts the connector.
"""

import inspect
import logging
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

from locking_dict import LockingDict

try:
    import simplejson as json
except:
    import json


class Connector(threading.Thread):
    """Checks the cluster for shards to tail.
    """
    def __init__(self, address, oplog_checkpoint, backend_url, ns_set,
                 u_key, auth_key, doc_manager=None, auth_username=None):
        file = inspect.getfile(inspect.currentframe())
        cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))
        if doc_manager is not None:
            if (doc_manager[0] is '/') or 'C:\\' in doc_manager:
                if ('win32' or 'win64') in sys.platform:
                    shutil.copy(doc_manager, cmd_folder + "\\doc_manager.py")
                else:
                    shutil.copy(doc_manager, cmd_folder + "/doc_manager.py")
            else:
                if ('win32' or 'win64') in sys.platform:
                    shutil.copy(cmd_folder + "\\doc_managers\\" + doc_manager,
                                cmd_folder + "\\doc_manager.py")
                else:
                    shutil.copy(cmd_folder + "/doc_managers/" + doc_manager,
                                cmd_folder + "/doc_manager.py")
        time.sleep(1)
        from doc_manager import DocManager
        super(Connector, self).__init__()

        #can_run is set to false when we join the thread
        self.can_run = True

        #The name of the file that stores the progress of the OplogThreads
        self.oplog_checkpoint = oplog_checkpoint

        #main address - either mongos for sharded setups or a primary otherwise
        self.address = address

        #The URL of the target system
        self.backend_url = backend_url

        #The set of relevant namespaces to consider
        self.ns_set = ns_set

        #The key that is a unique document identifier for the backend system.
        #Not necessarily the mongo unique key.
        self.u_key = u_key

        #Password for authentication
        self.auth_key = auth_key

        #Username for authentication
        self.auth_username = auth_username

        #The set of OplogThreads created
        self.shard_set = {}

        #Dict of OplogThread/timestmap pairs to record progress
        self.oplog_progress = LockingDict()

        try:
            if backend_url is None:
                self.doc_manager = DocManager()
            else:
                self.doc_manager = DocManager(self.backend_url)
        except SystemError:
            logging.critical("MongoConnector: Bad target system URL!")
            self.can_run = False
            return

        if self.oplog_checkpoint is not None:
            if not os.path.exists(self.oplog_checkpoint):
                logging.critical("MongoConnector: Can't find OplogProgress file!")
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
        os.rename(self.oplog_checkpoint, self.oplog_checkpoint + '++')
        dest = open(self.oplog_checkpoint, 'w')
        source = open(self.oplog_checkpoint + '++', 'r')

        # for each of the threads write to file
        self.oplog_progress.acquire_lock()

        for oplog, ts in self.oplog_progress.dict.items():
            oplog_str = str(oplog)
            timestamp = util.bson_ts_to_long(ts)
            json_str = json.dumps([oplog_str, timestamp])
            dest.write(json_str)

        self.oplog_progress.release_lock()

        dest.close()
        source.close()
        os.remove(self.oplog_checkpoint + '++')

    def read_oplog_progress(self):
        """Reads oplog progress from file provided by user
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

        count = 0
        for count in range(0, len(data), 2):
            oplog_str = data[count]
            ts = data[count + 1]
            self.oplog_progress.dict[oplog_str] = util.long_to_bson_ts(ts)
            #stored as bson_ts

        source.close()

    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        main_conn = pymongo.Connection(self.address)
        shard_coll = main_conn['config']['shards']

        self.read_oplog_progress()

        if shard_coll.find().count() == 0:
            #non sharded configuration
            try:
                main_conn.admin.command("isdbgrid")
                logging.error('MongoConnector: mongos has no shards!')
                self.doc_manager.stop()
                return
            except pymongo.errors.OperationFailure:
                pass
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

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler('mongo_connector_log.txt')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info('Beginning Mongo Connector')

    parser = optparse.OptionParser()

    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    parser.add_option("-m", "--main", action="store", type="string",
                      dest="main_addr", default="localhost:27217",
                      help="""Specify the mongos address, which is a """
                      """host:port pair, or for clusters with one shard,"""
                      """the primary's adderess. For example, `-m """
                      """localhost:27217` would be a valid argument"""
                      """to `-m`. It is not necessary to specify """
                      """double-quotes aroung the argument to `-m`. Don't """
                      """ use quotes around the address.""")

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

    #-b is to specify the URL to the backend engine being used.
    parser.add_option("-b", "--backend-url", action="store", type="string",
                      dest="url", default=None,
                      help="""Specify the URL to the backend engine being """
                      """used. For example, if you were using Solr out of """
                      """the box, you could use '-b """
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

    #-u is to specify the uniqueKey used by the backend,
    parser.add_option("-u", "--unique-key", action="store", type="string",
                      dest="u_key", default="_id", help=
                      """Used to specify the uniqueKey used by the backend."""
                      """The default is "_id", which can be noted by """
                      """  '-u _id'""")

    #-k is to specify the authentication key file. This file is used by mongos
    #to authenticate connections to the shards, and we'll use it in the oplog
    #threads.
    parser.add_option("-k", "--keyFile", action="store", type="string",
                      dest="auth_file", default=None, help=
                      """Used to specify the path to the authentication key"""
                      """file. This file is used by mongos to authenticate"""
                      """ connections to the shards, and we'll use it in the"""
                      """ oplog threads. If authentication is not used, then"""
                      """ this field can be left empty as the default """
                      """ is None.""")

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
                      """Used to specify the file in the /doc_managers"""
                      """folder that should be used as the doc manager."""
                      """Absolute paths also supported. By default, it will"""
                      """use the doc_manager_simulator.py file.""")

    (options, args) = parser.parse_args()

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
            logger.error('Could not parse authentication file!')
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
