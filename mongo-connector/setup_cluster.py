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

"""
Module with code to setup cluster and test oplog_manager functions.

This is the main tester method.
    All the functions can be called by finaltests.py
"""

import subprocess
import sys
import time
import os
import json

from pymongo import Connection
from pymongo.errors import ConnectionFailure, OperationFailure
from os import path
from util import retry_until_ok

""" Global path variables
"""
PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
                        "CONFIG": "27220", "MONGOS": "27217"}
PORTS_TWO = {"PRIMARY": "27317", "SECONDARY": "27318", "ARBITER": "27319",
                        "CONFIG": "27220", "MONGOS": "27217"}
SETUP_DIR = path.expanduser("~/mongo-connector")
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS_ONE["MONGOS"]


def remove_dir(path):
    """Remove supplied directory
    """
    command = ["rm", "-rf", path]
    subprocess.Popen(command).communicate()


def create_dir(path):
    """Create supplied directory
    """
    command = ["mkdir", "-p", path]
    subprocess.Popen(command).communicate()


def killMongoProc(host, port):
    """ Kill given port
    """
    try:
        conn = Connection(host, int(port))
        conn['admin'].command('shutdown', 1, force=True)
    except:
        cmd = ["pgrep -f \"" + str(port) + MONGOD_KSTR + "\" | xargs kill -9"]
        executeCommand(cmd)


def killMongosProc():
    """ Kill all mongos proc
    """
    cmd = ["pgrep -f \"" + MONGOS_KSTR + "\" | xargs kill -9"]
    executeCommand(cmd)


def killAllMongoProc(host, ports):
    """Kill any existing mongods
    """
    for port in ports.values():
        killMongoProc(host, port)


def startMongoProc(port, replSetName, data, log, key_file):
    """Create the replica set
    """
    CMD = ["mongod --fork --replSet " + replSetName + " --noprealloc --port "
           + port + " --dbpath " + DEMO_SERVER_DATA + data +
           " --shardsvr --rest --logpath " + DEMO_SERVER_LOG
           + log + " --logappend"]

    if key_file is not None:
        CMD[0] += " --keyFile " + key_file

    CMD[0] += " &"
    executeCommand(CMD)
    checkStarted(int(port))


def executeCommand(command):
    """Wait a little and then execute shell command
    """
    time.sleep(1)
    #return os.system(command)
    subprocess.Popen(command, shell=True)


#========================================= #
#   Helper functions to make sure we move  #
#   on only when we're good and ready      #
#========================================= #


def tryConnection(port):
    """Uses pymongo to try to connect to mongod
    """
    error = 0
    try:
        Connection('localhost', port)
    except Exception:
        error = 1
    return error


def checkStarted(port):
    """Checks if our the mongod has started
    """
    connected = False

    while not connected:
        error = tryConnection(port)
        if error:
            #Check every 1 second
            time.sleep(1)
        else:
            connected = True

#========================================= #
#   Start Cluster                          #
#========================================= #


def start_cluster(sharded=False, key_file=None, use_mongos=True):
        """Sets up cluster with 1 shard, replica set with 3 members
        """
        # Kill all spawned mongods
        killAllMongoProc('localhost', PORTS_ONE)
        killAllMongoProc('localhost', PORTS_TWO)

        # Kill all spawned mongos
        killMongosProc()

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
        startMongoProc(PORTS_ONE["PRIMARY"], "demo-repl", "/replset1a",
                       "/replset1a.log", key_file)
        startMongoProc(PORTS_ONE["SECONDARY"], "demo-repl", "/replset1b",
                       "/replset1b.log", key_file)
        startMongoProc(PORTS_ONE["ARBITER"], "demo-repl", "/replset1c",
                       "/replset1c.log", key_file)

        if sharded:
            startMongoProc(PORTS_TWO["PRIMARY"], "demo-repl-2", "/replset2a",
                           "/replset2a.log", key_file)
            startMongoProc(PORTS_TWO["SECONDARY"], "demo-repl-2", "/replset2b",
                           "/replset2b.log", key_file)
            startMongoProc(PORTS_TWO["ARBITER"], "demo-repl-2", "/replset2c",
                           "/replset2c.log", key_file)

        # Setup config server
        CMD = ["mongod --oplogSize 500 --fork --configsvr --noprealloc --port "
               + PORTS_ONE["CONFIG"] + " --dbpath " + DEMO_SERVER_DATA +
               "/config1 --rest --logpath " +
               DEMO_SERVER_LOG + "/config1.log --logappend"]

        if key_file is not None:
            CMD[0] += " --keyFile " + key_file

        CMD[0] += " &"
        executeCommand(CMD)
        checkStarted(int(PORTS_ONE["CONFIG"]))

        # Setup the mongos, same mongos for both shards
        CMD = ["mongos --port " + PORTS_ONE["MONGOS"] +
               " --fork --configdb localhost:" +
               PORTS_ONE["CONFIG"] + " --chunkSize 1  --logpath " +
               DEMO_SERVER_LOG + "/mongos1.log --logappend"]

        if key_file is not None:
            CMD[0] += " --keyFile " + key_file

        CMD[0] += " &"
        if use_mongos:
            executeCommand(CMD)
            checkStarted(int(PORTS_ONE["MONGOS"]))

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
                print 'Could not add shard to mongos'
                sys.exit(1)

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
                print 'Could not add shard to mongos'
                sys.exit(1)

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
