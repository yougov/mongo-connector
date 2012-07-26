"""Discovers the mongo cluster and starts the connector.
"""

import logging
import os
import re
import simplejson as json
import time

from backend_simulator import BackendSimulator
from doc_manager import DocManager
from pymongo import Connection
from oplog_manager import OplogThread
from optparse import OptionParser
from simplejson import JSONDecodeError
from sys import exit
from threading import Thread
from util import bson_ts_to_long, long_to_bson_ts
from bson.timestamp import Timestamp


class Connector(Thread):
    """Checks the cluster for shards to tail.
    """

    def __init__(self, address, oplog_checkpoint, backend_url, ns_set, u_key,
                 auth_key):
        super(Connector, self).__init__()
        self.can_run = True
        self.oplog_checkpoint = oplog_checkpoint
        self.address = address
        self.backend_url = backend_url
        self.ns_set = ns_set
        self.u_key = u_key
        self.auth_key = auth_key
        self.shard_set = {}
        self.oplog_progress_dict = {}

        if self.backend_url is None:
            self.doc_manager = BackendSimulator()
        else:
            self.doc_manager = DocManager(self.backend_url)

        if self.doc_manager is None:
            logging.critical('Bad backend URL!')
            return

    def join(self):
        """ Joins thread, stops it from running
        """

        self.can_run = False
        Thread.join(self)

    def write_oplog_progress(self):
        """ Writes oplog progress to file provided by user
        """

        if self.oplog_checkpoint is None:
                return None

        ofile = file(self.oplog_checkpoint, 'r+')

        os.rename(self.oplog_checkpoint, self.oplog_checkpoint + '~')
        dest = open(self.oplog_checkpoint, 'w')
        source = open(self.oplog_checkpoint + '~', 'r')

        for oplog, ts in self.oplog_progress_dict.items():
            oplog_str = str(oplog)
            timestamp = bson_ts_to_long(ts)
            json_str = json.dumps([oplog_str, timestamp])
            dest.write(json_str)

        dest.close()
        source.close()
        os.remove(self.oplog_checkpoint + '~')

    def read_oplog_progress(self):
        """Reads oplog progress from file provided by user
        """

        if self.oplog_checkpoint is None:
            return None

        source = open(self.oplog_checkpoint, 'r')
        try:
            data = json.load(source)
        except JSONDecodeError:                 # empty file
            return None

        count = 0
        while count < len(data):
            oplog_str = data[count]
            ts = data[count + 1]
            self.oplog_progress_dict[oplog_str] = long_to_bson_ts(ts)
            #stored as bson_ts
            count = count + 2                        # skip to next set

       # return self.checkpoint.commit_ts

    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        mongos_conn = Connection(self.address)
        shard_coll = mongos_conn['config']['shards']

        self.read_oplog_progress()
        while self.can_run is True:

            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                if shard_id in self.shard_set:
                    self.write_oplog_progress()
                    time.sleep(1)
                    continue

                repl_set, hosts = shard_doc['host'].split('/')
                shard_conn = Connection(hosts, replicaset=repl_set)
                oplog_coll = shard_conn['local']['oplog.rs']
                oplog = OplogThread(shard_conn, self.address, oplog_coll, True,
                                    self.doc_manager, self.oplog_progress_dict,
                                    self.ns_set, self.auth_key)
                self.shard_set[shard_id] = oplog
                logging.info('MongoInternal: Starting connection thread %s' %
                             shard_conn)
                oplog.start()

        #time to stop running
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

    parser = OptionParser()

    #-m is for the mongos address, which is a host:port pair.
    parser.add_option("-m", "--mongos", action="store", type="string",
                      dest="mongos_addr", default="localhost:27217")

    #-o is to specify the oplog-config file. This file is used by the system
    #to store the last timestamp read on a specific oplog. This allows for
    #quick recovery from failure.
    parser.add_option("-o", "--oplog-ts", action="store", type="string",
                      dest="oplog_config", default="config.txt")

    #-b is to specify the URL to the backend engine being used.
    parser.add_option("-b", "--backend-url", action="store", type="string",
                      dest="url", default="")

    #-n is to specify the namespaces we want to consider. The default
    #considers 'test.test' and 'alpha.foo'
    parser.add_option("-n", "--namespace-set", action="store", type="string",
                      dest="ns_set", default="test.test,alpha.foo")

    #-u is to specify the uniqueKey used by the backend,
    parser.add_option("-u", "--unique-key", action="store", type="string",
                      dest="u_key", default="_id")

    #-a is to specify the authentication key file. This file is used by mongos
    #to authenticate connections to the shards, and we'll use it in the oplog
    #threads.
    parser.add_option("-a", "--auth-file", action="store", type="string",
                      dest="auth_file", default=None)

    (options, args) = parser.parse_args()

    try:
        ns_set = options.ns_set.split(',')
    except:
        logger.error('Namespaces must be separated by commas!')
        exit(1)

    key = None
    if options.auth_file is not None:
        try:
            file = open(options.auth_file)
            key = file.read()
            re.sub(r'\s', '', key)
        except:
            logger.error('Could not parse authentication file!')
            exit(1)

    ct = Connector(options.mongos_addr, options.oplog_config, options.url,
                ns_set, options.u_key, key)
    ct.start()
