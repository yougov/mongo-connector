"""Discovers the mongo cluster and starts the daemon.
"""

import time
import logging

from doc_manager import DocManager
from pymongo import Connection
from oplog_manager import OplogThread
from optparse import OptionParser
from sys import exit
from threading import Thread


class Daemon(Thread):
    """Checks the cluster for shards to tail.
    """

    def __init__(self, address, oplog_checkpoint, backend_url, ns_set, u_key):
        super(Daemon, self).__init__()
        self.can_run = True
        self.oplog_checkpoint = oplog_checkpoint
        self.address = address
        self.backend_url = backend_url
        self.ns_set = ns_set
        self.u_key = u_key
        self.shard_set = {}

    def stop(self):
        self.can_run = False

    def run(self):
        """Discovers the mongo cluster and creates a thread for each primary.
        """
        mongos_conn = Connection(self.address)
        shard_coll = mongos_conn['config']['shards']
        doc_manager = DocManager(self.backend_url)
        if doc_manager is None:
            logging.critical('Bad backend URL!')
            return

        while self.can_run is True:

            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                if shard_id in self.shard_set:
                    time.sleep(2)
                    continue

                shard_conn = Connection(shard_doc['host'])
                oplog_coll = shard_conn['local']['oplog.rs']
                oplog = OplogThread(shard_conn, self.address, oplog_coll, True,
                                    doc_manager, self.oplog_checkpoint,
                                    self.ns_set)
                self.shard_set[shard_id] = oplog
                logging.info('MongoInternal: Starting connection thread %s' %
                             shard_conn)
                oplog.start()

        #time to stop running
        for thread in self.shard_set.values():
            thread.stop()


if __name__ == '__main__':

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
    parser.add_option("-o", "--oplog-config", action="store", type="string",
                      dest="oplog_config", default="./config.txt")

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

    (options, args) = parser.parse_args()

    try:
        ns_set = options.ns_set.split(',')
    except:
        logger.error('Namespaces must be separated by commas!')
        exit(1)

    dt = Daemon(options.mongos_addr, options.oplog_config, options.url,
                ns_set, options.u_key)
    dt.start()
