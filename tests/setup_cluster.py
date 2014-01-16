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

import subprocess
import sys
import time
import os
import inspect

try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection    

sys.path[0:0] = [""]

from pymongo.errors import ConnectionFailure, OperationFailure, AutoReconnect
from os import path
from mongo_connector.util import retry_until_ok

PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
                        "CONFIG": "27220", "MONGOS": "27217"}
PORTS_TWO = {"PRIMARY": "27317", "SECONDARY": "27318", "ARBITER": "27319",
                        "CONFIG": "27220", "MONGOS": "27217"}

CURRENT_DIR = inspect.getfile(inspect.currentframe())
CMD_DIR = os.path.realpath(os.path.abspath(os.path.split(CURRENT_DIR)[0]))
SETUP_DIR = path.expanduser(CMD_DIR)
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS_ONE["MONGOS"]


def remove_dir(dir_path):
    """Remove supplied directory
    """
    command = ["rm", "-rf", dir_path]
    subprocess.Popen(command).communicate()


def create_dir(dir_path):
    """Create supplied directory
    """
    command = ["mkdir", "-p", dir_path]
    subprocess.Popen(command).communicate()


def kill_mongo_proc(host, port):
    """ Kill given port
    """
    conn = None
    try:
        conn = Connection(host, int(port))
    except ConnectionFailure:
        cmd = ["pgrep -f \"" + str(port) + MONGOD_KSTR + "\" | xargs kill -9"]
        execute_command(cmd)
    if conn:
        try:    
            conn['admin'].command('shutdown', 1, force=True)
        except (AutoReconnect, ConnectionFailure):    
            pass

def kill_mongos_proc():
    """ Kill all mongos proc
    """
    cmd = ["pgrep -f \"" + MONGOS_KSTR + "\" | xargs kill -9"]
    execute_command(cmd)


def kill_all_mongo_proc(host, ports):
    """Kill any existing mongods
    """
    for port in ports.values():
        kill_mongo_proc(host, port)

def start_single_mongod_instance(port, data, log):
    """ Creates a single mongod instance 
    """
    remove_dir(DEMO_SERVER_DATA + data)
    create_dir(DEMO_SERVER_DATA + data) 
    create_dir(DEMO_SERVER_LOG) 
    cmd = ("mongod --fork --noprealloc --port %s --dbpath %s/%s "
           "--logpath %s/%s --logappend" %
          (port, DEMO_SERVER_DATA, data, DEMO_SERVER_LOG, log))        
    execute_command(cmd)
    check_started(int(port))

def kill_all():
    """Kills all running mongod and mongos instances
    """
    kill_all_mongo_proc("localhost", PORTS_ONE)
    kill_all_mongo_proc("localhost", PORTS_TWO)
    kill_mongos_proc()
    remove_dir(DEMO_SERVER_LOG)
    remove_dir(DEMO_SERVER_DATA)

def start_mongo_proc(port, repl_set_name, data, log, key_file):
    """Create the replica set
    """
    cmd = ("mongod --fork --replSet %s --noprealloc --port %s --dbpath %s%s"
           " --shardsvr --nojournal --rest --logpath %s%s --logappend" %
           (repl_set_name, port, DEMO_SERVER_DATA, data, DEMO_SERVER_LOG, log))

    if key_file is not None:
        cmd += " --keyFile " + key_file

    cmd += " &"
    execute_command(cmd)
    #if port != PORTS_ONE["ARBITER"] and port != PORTS_TWO["ARBITER"]:
    check_started(int(port))


def execute_command(command):
    """Wait a little and then execute shell command
    """
    time.sleep(1)
    #return os.system(command)
    subprocess.Popen(command, shell=True)


#========================================= #
#   Helper functions to make sure we move  #
#   on only when we're good and ready      #
#========================================= #


def check_started(port):
    """Checks if our the mongod has started
    """
    while True:
        try:
            Connection('localhost', port)
            break
        except ConnectionFailure:    
            time.sleep(1)

#========================================= #
#   Start Cluster                          #
#========================================= #


def start_cluster(sharded=False, key_file=None, use_mongos=True):
    """Sets up cluster with 1 shard, replica set with 3 members
    """
    # Kill all spawned mongods
    kill_all_mongo_proc('localhost', PORTS_ONE)
    kill_all_mongo_proc('localhost', PORTS_TWO)

    # Kill all spawned mongos
    kill_mongos_proc()

    # reset data dirs
    remove_dir(DEMO_SERVER_LOG)
    remove_dir(DEMO_SERVER_DATA)

    create_dir(DEMO_SERVER_DATA + "/standalone/journal")
    create_dir(DEMO_SERVER_DATA + "/replset1a/journal")
    create_dir(DEMO_SERVER_DATA + "/replset1b/journal")
    create_dir(DEMO_SERVER_DATA + "/replset1c/journal")

    if sharded:
        create_dir(DEMO_SERVER_DATA + "/replset2a/journal")
        create_dir(DEMO_SERVER_DATA + "/replset2b/journal")
        create_dir(DEMO_SERVER_DATA + "/replset2c/journal")

    create_dir(DEMO_SERVER_DATA + "/shard1a/journal")
    create_dir(DEMO_SERVER_DATA + "/shard1b/journal")
    create_dir(DEMO_SERVER_DATA + "/config1/journal")
    create_dir(DEMO_SERVER_LOG)

    # Create the replica set
    start_mongo_proc(PORTS_ONE["PRIMARY"], "demo-repl", "/replset1a",
                   "/replset1a.log", key_file)
    start_mongo_proc(PORTS_ONE["SECONDARY"], "demo-repl", "/replset1b",
                   "/replset1b.log", key_file)
    start_mongo_proc(PORTS_ONE["ARBITER"], "demo-repl", "/replset1c",
                   "/replset1c.log", key_file)

    if sharded:
        start_mongo_proc(PORTS_TWO["PRIMARY"], "demo-repl-2", "/replset2a",
                       "/replset2a.log", key_file)
        start_mongo_proc(PORTS_TWO["SECONDARY"], "demo-repl-2", "/replset2b",
                       "/replset2b.log", key_file)
        start_mongo_proc(PORTS_TWO["ARBITER"], "demo-repl-2", "/replset2c",
                       "/replset2c.log", key_file)

    # Setup config server
    cmd = ("mongod --oplogSize 500 --fork --configsvr --noprealloc --port "
           + PORTS_ONE["CONFIG"] + " --dbpath " + DEMO_SERVER_DATA +
           "/config1 --rest --logpath " +
           DEMO_SERVER_LOG + "/config1.log --logappend")

    if key_file is not None:
        cmd += " --keyFile " + key_file

    cmd += " &"
    execute_command(cmd)
    check_started(int(PORTS_ONE["CONFIG"]))

    # Setup the mongos, same mongos for both shards
    cmd = ["mongos --port " + PORTS_ONE["MONGOS"] +
           " --fork --configdb localhost:" +
           PORTS_ONE["CONFIG"] + " --chunkSize 1  --logpath " +
           DEMO_SERVER_LOG + "/mongos1.log --logappend"]

    if key_file is not None:
        cmd += " --keyFile " + key_file

    cmd += " &"
    if use_mongos:
        execute_command(cmd)
        check_started(int(PORTS_ONE["MONGOS"]))

    # configuration for replSet 1
    config = {'_id': "demo-repl", 'members': [
              {'_id': 0, 'host': "localhost:27117"},
              {'_id': 1, 'host': "localhost:27118"},
              {'_id': 2, 'host': "localhost:27119",
               'arbiterOnly': 'true'}]}

    # configuration for replSet 2, not always used
    config2 = {'_id': "demo-repl-2", 'members': [
               {'_id': 0, 'host': "localhost:27317"},
               {'_id': 1, 'host': "localhost:27318"},
               {'_id': 2, 'host': "localhost:27319",
                'arbiterOnly': 'true'}]}

    primary = Connection('localhost:27117')
    if use_mongos:
        mongos = Connection('localhost:27217')
    primary.admin.command("replSetInitiate", config)

    # ensure that the replSet is properly configured
    while retry_until_ok(primary.admin.command,
                         "replSetGetStatus")['myState'] == 0:
        time.sleep(1)

    if use_mongos:
        counter = 100
        while counter > 0:
            try:
                mongos.admin.command("addShard",
                                     "demo-repl/localhost:27117")
                break
            except OperationFailure:            # replSet not ready yet
                counter -= 1
                time.sleep(1)

        if counter == 0:
            return False

    if sharded:
        primary2 = Connection('localhost:27317')
        primary2.admin.command("replSetInitiate", config2)

        while retry_until_ok(primary2.admin.command,
                             "replSetGetStatus")['myState'] == 0:
            time.sleep(1)

        counter = 100
        while counter > 0:
            try:
                admin_db = mongos.admin
                admin_db.command("addShard",
                                 "demo-repl-2/localhost:27317", maxSize=1)
                break
            except OperationFailure:            # replSet not ready yet
                counter -= 1
                time.sleep(1)

        if counter == 0:
            return False

        # shard on the alpha.foo collection
        admin_db = mongos.admin
        admin_db.command("enableSharding", "alpha")
        admin_db.command("shardCollection", "alpha.foo", key={"_id": 1})

    primary = Connection('localhost:27117')
    admin = primary['admin']
    while admin.command("isMaster")['ismaster'] is False:
        time.sleep(1)
    secondary = Connection('localhost:27118')
    while secondary.admin.command("replSetGetStatus")['myState'] is not 2:
        time.sleep(1)
    return True
