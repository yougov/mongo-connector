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
Module with code to setup cluster and test oplog_manager functions.

This is the main tester method.
    All the functions can be called by finaltests.py
"""

import inspect
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

CURRENT_DIR = inspect.getfile(inspect.currentframe())
CMD_DIR = os.path.realpath(os.path.abspath(os.path.split(CURRENT_DIR)[0]))
SETUP_DIR = os.path.expanduser(CMD_DIR)
DEMO_SERVER_DATA = os.path.join(SETUP_DIR, "data")
DEMO_SERVER_LOG = os.path.join(SETUP_DIR, "logs")
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS_ONE["MONGOS"]
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


def kill_mongo_proc(host, port):
    """ Kill given port
    """
    try:
        conn = MongoClient(host, int(port))
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


def kill_all_mongo_proc(host, ports):
    """Kill any existing mongods
    """
    for port in ports.values():
        kill_mongo_proc(host, port)


def kill_all():
    """Kills all running mongod and mongos instances
    """
    kill_all_mongo_proc("localhost", PORTS_ONE)
    kill_all_mongo_proc("localhost", PORTS_TWO)
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
    cmd = ("mongod  --noprealloc --port %s --dbpath %s "
           "--logpath %s --logappend" %
           (port,
            os.path.join(DEMO_SERVER_DATA, data),
            os.path.join(DEMO_SERVER_LOG, log)))
    execute_command(cmd)
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

    execute_command(cmd)
    retry_until_ok(lambda: MongoClient('localhost', int(port)))


def execute_command(command):
    """Wait a little and then execute shell command
    """
    subprocess.Popen(command, shell=True)


#========================================= #
#   Start Cluster                          #
#========================================= #


@clean_mess_on_error
def start_cluster(sharded=False, use_mongos=True):
    """Sets up cluster with 1 shard, replica set with 3 members
    """
    # Kill all spawned mongo[ds]
    kill_all_mongo_proc('localhost', PORTS_ONE)
    kill_all_mongo_proc('localhost', PORTS_TWO)

    # reset data dirs
    remove_dir(DEMO_SERVER_LOG)
    remove_dir(DEMO_SERVER_DATA)

    # create journal directories
    dirs = ["standalone", "replset1a", "replset1b",
            "replset1c", "shard1a", "shard1b", "config1"]
    if sharded:
        dirs += ["replset2a", "replset2b", "replset2c"]
    for d in dirs:
        create_dir(os.path.join(DEMO_SERVER_DATA, d, "journal"))

    # log directory
    create_dir(DEMO_SERVER_LOG)

    # Create the replica set
    start_mongo_proc(PORTS_ONE["PRIMARY"], "demo-repl",
                     "replset1a", "replset1a.log")
    start_mongo_proc(PORTS_ONE["SECONDARY"], "demo-repl",
                     "replset1b", "replset1b.log")
    start_mongo_proc(PORTS_ONE["ARBITER"], "demo-repl",
                     "replset1c", "replset1c.log")

    if sharded:
        start_mongo_proc(PORTS_TWO["PRIMARY"], "demo-repl-2",
                         "replset2a", "replset2a.log")
        start_mongo_proc(PORTS_TWO["SECONDARY"], "demo-repl-2",
                         "replset2b", "replset2b.log")
        start_mongo_proc(PORTS_TWO["ARBITER"], "demo-repl-2",
                         "replset2c", "replset2c.log")

    if use_mongos:
        # Setup config server
        cmd = ("mongod --oplogSize 500 --configsvr "
               "--noprealloc --port %s "
               "--dbpath %s --logpath %s --logappend" % (
                   PORTS_ONE["CONFIG"],
                   os.path.join(DEMO_SERVER_DATA, "config1"),
                   os.path.join(DEMO_SERVER_LOG, "config1.log")))

        if KEY_FILE is not None:
            cmd += " --keyFile " + KEY_FILE

        execute_command(cmd)
        retry_until_ok(
            lambda: MongoClient('localhost', int(PORTS_ONE['CONFIG'])))

        # Setup the mongos, same mongos for both shards
        cmd = ("mongos --port %s --configdb localhost:%s"
               " --chunkSize 1  --logpath %s --logappend" % (
                   PORTS_ONE["MONGOS"],
                   PORTS_ONE["CONFIG"],
                   os.path.join(DEMO_SERVER_LOG, "mongos1.log")))

        if KEY_FILE is not None:
            cmd += " --keyFile " + KEY_FILE

        execute_command(cmd)
        retry_until_ok(
            lambda: MongoClient('localhost', int(PORTS_ONE["MONGOS"])))

    # configuration for replSet 1
    config = {'_id': "demo-repl", 'members': [
        {'_id': 0, 'host': "localhost:%s" % PORTS_ONE["PRIMARY"]},
        {'_id': 1, 'host': "localhost:%s" % PORTS_ONE["SECONDARY"]},
        {'_id': 2, 'host': "localhost:%s" % PORTS_ONE["ARBITER"],
         'arbiterOnly': 'true'}
    ]}

    # configuration for replSet 2, not always used
    config2 = {'_id': "demo-repl-2", 'members': [
        {'_id': 0, 'host': "localhost:%s" % PORTS_TWO["PRIMARY"]},
        {'_id': 1, 'host': "localhost:%s" % PORTS_TWO["SECONDARY"]},
        {'_id': 2, 'host': "localhost:%s" % PORTS_TWO["ARBITER"],
         'arbiterOnly': 'true'}
    ]}

    primary = MongoClient('localhost:%s' % PORTS_ONE["PRIMARY"])
    if use_mongos:
        mongos = MongoClient('localhost:%s' % PORTS_ONE["MONGOS"])
    primary.admin.command("replSetInitiate", config)

    # wait for primary to come up
    def primary_up():
        return retry_until_ok(primary.admin.command,
                              "isMaster")['ismaster']
    assert_soon(primary_up)

    if use_mongos:
        retry_until_ok(
            mongos.admin.command,
            "addShard",
            "demo-repl/localhost:%s" % PORTS_ONE["PRIMARY"]
        )

    if sharded:
        primary2 = MongoClient('localhost:%s' % PORTS_TWO["PRIMARY"])
        primary2.admin.command("replSetInitiate", config2)

        # Wait for primary on shard 2 to come up
        def primary_up():
            return retry_until_ok(primary2.admin.command,
                                  "isMaster")['ismaster']
        assert_soon(primary_up)

        retry_until_ok(
            mongos.admin.command,
            "addShard",
            "demo-repl-2/localhost:%s" % PORTS_TWO["PRIMARY"],
            maxSize=1
        )

    primary = MongoClient('localhost:27117')
    admin = primary['admin']
    assert_soon(lambda: admin.command("isMaster")['ismaster'])
    secondary = MongoClient('localhost:%s' % PORTS_ONE["SECONDARY"])
    c = lambda: secondary.admin.command("replSetGetStatus")["myState"] == 2
    assert_soon(c, max_tries=120)

    return True
