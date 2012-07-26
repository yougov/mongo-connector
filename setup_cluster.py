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
from pymongo.errors import ConnectionFailure
from os import path

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


def start_cluster(sharded=False, key_file=None):
        """Sets up cluster with 1 shard, replica set with 3 members
        """
        # Kill all spawned mongods
        killAllMongoProc('localhost', PORTS_ONE)
        killAllMongoProc('localhost', PORTS_TWO)

        # Kill all spawned mongos
        killMongosProc()

        remove_dir(DEMO_SERVER_LOG)
        remove_dir(DEMO_SERVER_DATA)

        create_dir(DEMO_SERVER_DATA + "/standalone/journal")

        create_dir(DEMO_SERVER_DATA + "/replset1a/journal")
        create_dir(DEMO_SERVER_DATA + "/replset1b/journal")
        create_dir(DEMO_SERVER_DATA + "/replset1c/journal")

        if sharded is True:
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
        executeCommand(CMD)
        checkStarted(int(PORTS_ONE["MONGOS"]))

        # Configure the shards and begin load simulation
        if sharded:
            cmd1 = "mongo --port " + PORTS_ONE["PRIMARY"] + " "
            cmd1 = cmd1 + SETUP_DIR + "/setup/configReplSetSharded1.js"
            cmd3 = "mongo --port  " + PORTS_ONE["MONGOS"] + " "
            cmd3 = cmd3 + SETUP_DIR + "/setup/configMongosSharded.js"
        else:
            cmd1 = "mongo --port " + PORTS_ONE["PRIMARY"] + " "
            cmd1 = cmd1 + SETUP_DIR + "/setup/configReplSet.js"
            cmd3 = "mongo --port  " + PORTS_ONE["MONGOS"] + " "
            cmd3 = cmd3 + SETUP_DIR + "/setup/configMongos.js"

        cmd2 = "mongo --port " + PORTS_TWO["PRIMARY"] + " "
        cmd2 = cmd2 + SETUP_DIR + "/setup/configReplSetSharded2.js"

        subprocess.call(cmd1, shell=True)
        conn = Connection('localhost:' + PORTS_ONE["PRIMARY"])
        sec_conn = Connection('localhost:' + PORTS_ONE["SECONDARY"])

        while conn['admin'].command("isMaster")['ismaster'] is False:
                time.sleep(1)

        while sec_conn['admin'].command("replSetGetStatus")['myState'] != 2:
                time.sleep(1)

        if sharded:
            subprocess.call(cmd2, shell=True)
            conn = Connection('localhost:' + PORTS_TWO["PRIMARY"])
            sec_conn = Connection('localhost:' + PORTS_TWO["SECONDARY"])

            while conn['admin'].command("isMaster")['ismaster'] is False:
                time.sleep(1)

            #need to look a bit more carefully here
            admin_db = sec_conn['admin']
            while admin_db.command("replSetGetStatus")['myState'] != 2:
                time.sleep(1)

        subprocess.call(cmd3, shell=True)
