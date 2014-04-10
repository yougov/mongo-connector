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

"""
Utilities for spawning and killing mongodb clusters for use with the tests
"""

import os
import psutil
import shutil
import subprocess

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from mongo_connector.util import retry_until_ok
from tests.util import assert_soon

PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
                        "CONFIG": "27220", "MONGOS": "27217"}
PORTS_TWO = {"PRIMARY": "27317", "SECONDARY": "27318", "ARBITER": "27319",
                        "CONFIG": "27220", "MONGOS": "27217"}

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO_SERVER_DATA = os.path.join(HERE, "data")
DEMO_SERVER_LOG = os.path.join(HERE, "logs")
KEY_FILE = os.environ.get("KEY_FILE")


def remove_dir(dir_path):
    """Remove supplied directory
    """
    shutil.rmtree(dir_path, ignore_errors=True)


def create_dir(dir_path):
    """Create supplied directory
    """
    try:
        os.makedirs(dir_path)
    except OSError:     # directory may exist already
        pass


def kill_mongo_proc(port):
    """ Kill given port
    """
    try:
        conn = MongoClient('localhost', int(port))
    except ConnectionFailure:
        for proc in psutil.process_iter():
            try:
                cmdline = proc.cmdline()
                if len(cmdline) > 0:
                    if "mongo" in cmdline[0] and port in cmdline:
                        proc.terminate()
                        break
            except psutil.AccessDenied:
                pass
    else:
        try:
            conn['admin'].command('shutdown', 1, force=True)
        except ConnectionFailure:
            pass


def kill_all():
    """Kills all running mongod and mongos instances
    """
    for port in PORTS_ONE.values():
        kill_mongo_proc(port)
    for port in PORTS_TWO.values():
        kill_mongo_proc(port)
    remove_dir(DEMO_SERVER_LOG)
    remove_dir(DEMO_SERVER_DATA)


def clean_mess_on_error(func):
    def wrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            kill_all()
            raise
    return wrap


@clean_mess_on_error
def start_single_mongod_instance(port, data, log):
    """ Creates a single mongod instance
    """
    remove_dir(os.path.join(DEMO_SERVER_DATA, data))
    create_dir(os.path.join(DEMO_SERVER_DATA, data))
    create_dir(DEMO_SERVER_LOG)
    cmd = ("mongod --noprealloc --port %s --dbpath %s "
           "--logpath %s --logappend" %
           (port,
            os.path.join(DEMO_SERVER_DATA, data),
            os.path.join(DEMO_SERVER_LOG, log)))
    subprocess.Popen(cmd, shell=True)
    retry_until_ok(lambda: MongoClient('localhost', int(port)))


@clean_mess_on_error
def start_mongo_proc(port, repl_set_name, data, log):
    """Create the replica set
    """
    cmd = ("mongod --replSet %s --noprealloc --port %s --dbpath %s"
           " --shardsvr --nojournal --logpath %s --logappend"
           " --setParameter enableTestCommands=1" %
           (repl_set_name, port,
            os.path.join(DEMO_SERVER_DATA, data),
            os.path.join(DEMO_SERVER_LOG, log)))

    if KEY_FILE is not None:
        cmd += " --keyFile " + KEY_FILE

    subprocess.Popen(cmd, shell=True)
    retry_until_ok(lambda: MongoClient('localhost', int(port)))


#========================================= #
#   Start Cluster                          #
#========================================= #


@clean_mess_on_error
def start_cluster(sharded=False):
    """Sets up cluster with 1 shard, replica set with 3 members
    """

    # Kill all spawned mongo[ds] and remove data and log directories
    kill_all()

    # Create data and log directories
    dirs = ["replset1a", "replset1b", "replset1c"]
    if sharded:
        dirs += ["replset2a", "replset2b", "replset2c", "config"]
    for d in dirs:
        create_dir(os.path.join(DEMO_SERVER_DATA, d, "journal"))
    create_dir(DEMO_SERVER_LOG)

    # Create the first replica set
    start_mongo_proc(PORTS_ONE["PRIMARY"], "demo-repl",
                     "replset1a", "replset1a.log")
    start_mongo_proc(PORTS_ONE["SECONDARY"], "demo-repl",
                     "replset1b", "replset1b.log")
    start_mongo_proc(PORTS_ONE["ARBITER"], "demo-repl",
                     "replset1c", "replset1c.log")
    primary = MongoClient('localhost:%s' % PORTS_ONE["PRIMARY"])
    primary.admin.command(
        'replSetInitiate',
        {
            '_id': "demo-repl", 'members': [
                {'_id': 0, 'host': "localhost:%s" % PORTS_ONE["PRIMARY"]},
                {'_id': 1, 'host': "localhost:%s" % PORTS_ONE["SECONDARY"]},
                {'_id': 2, 'host': "localhost:%s" % PORTS_ONE["ARBITER"],
                 'arbiterOnly': 'true'}
            ]
        }
    )

    # wait for primary to come up
    def primary_up():
        return retry_until_ok(primary.admin.command,
                              "isMaster")['ismaster']
    assert_soon(primary_up)

    if sharded:
        # Create a second replica set
        start_mongo_proc(PORTS_TWO["PRIMARY"], "demo-repl-2",
                         "replset2a", "replset2a.log")
        start_mongo_proc(PORTS_TWO["SECONDARY"], "demo-repl-2",
                         "replset2b", "replset2b.log")
        start_mongo_proc(PORTS_TWO["ARBITER"], "demo-repl-2",
                         "replset2c", "replset2c.log")
        primary2 = MongoClient('localhost:%s' % PORTS_TWO["PRIMARY"])
        primary2.admin.command(
            "replSetInitiate",
            {
                '_id': "demo-repl-2", 'members': [
                    {'_id': 0, 'host': "localhost:%s" % PORTS_TWO["PRIMARY"]},
                    {'_id': 1, 'host': "localhost:%s" % PORTS_TWO["SECONDARY"]},
                    {'_id': 2, 'host': "localhost:%s" % PORTS_TWO["ARBITER"],
                     'arbiterOnly': 'true'}
                ]
            }
        )

        # Wait for primary on shard 2 to come up
        def primary_up():
            return retry_until_ok(primary2.admin.command,
                                  "isMaster")['ismaster']
        assert_soon(primary_up)

        # Setup a config server
        cmd = ("mongod --oplogSize 500 --configsvr "
               "--noprealloc --port %s "
               "--dbpath %s --logpath %s --logappend" % (
                   PORTS_ONE["CONFIG"],
                   os.path.join(DEMO_SERVER_DATA, "config"),
                   os.path.join(DEMO_SERVER_LOG, "config1.log")))

        if KEY_FILE is not None:
            cmd += " --keyFile " + KEY_FILE

        subprocess.Popen(cmd, shell=True)
        retry_until_ok(
            lambda: MongoClient('localhost', int(PORTS_ONE['CONFIG'])))

        # Setup the mongos
        cmd = ("mongos --port %s --configdb localhost:%s"
               " --chunkSize 1  --logpath %s --logappend" % (
                   PORTS_ONE["MONGOS"],
                   PORTS_ONE["CONFIG"],
                   os.path.join(DEMO_SERVER_LOG, "mongos1.log")))

        if KEY_FILE is not None:
            cmd += " --keyFile " + KEY_FILE

        subprocess.Popen(cmd, shell=True)
        retry_until_ok(
            lambda: MongoClient('localhost', int(PORTS_ONE["MONGOS"])))

        mongos = MongoClient('localhost:%s' % PORTS_ONE["MONGOS"])
        retry_until_ok(
            mongos.admin.command,
            "addShard",
            "demo-repl/localhost:%s" % PORTS_ONE["PRIMARY"]
        )

        retry_until_ok(
            mongos.admin.command,
            "addShard",
            "demo-repl-2/localhost:%s" % PORTS_TWO["PRIMARY"],
            maxSize=1
        )

    return True
