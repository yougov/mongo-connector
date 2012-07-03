"""
(C) Copyright 2012, 10gen
"""

import subprocess
import sys
import time
from pymongo import Connection
from pymongo.errors import ConnectionFailure
from os import path

# Global path variables
SETUP_DIR = path.expanduser("~/mongo-connector/mongo-connector/test")
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port 27220"
MONGOD_PORTS = ["27117", "27118", "27119", "27218",
                 "27219", "27220", "27017"]
                 
def killMongoProc(port):
    cmd = ["pgrep -f \"" + str(port) + MONGOD_KSTR + "\" | xargs kill -9"]
    executeCommand(cmd)
    
def killMongosProc():
    cmd = ["pgrep -f \"" + MONGOS_KSTR + "\" | xargs kill -9"]
    executeCommand(cmd)

def killAllMongoProc():
    """Kill any existing mongods"""
    for port in MONGOD_PORTS:
        killMongoProc(port)

def startMongoProc(port):
    CMD = ["mongod --oplogSize 500 --fork --noprealloc --port " + str(port) + " --dbpath " +
       DEMO_SERVER_DATA + "/standalone --rest --logpath " +
       DEMO_SERVER_LOG + "/standalone.log &"]
    executeCommand(CMD)
    checkStarted(port)
    
def remove_dir(path):
    """Remove supplied directory"""
    command = ["rm", "-rf", path]
    subprocess.Popen(command).communicate()

remove_dir(DEMO_SERVER_LOG)
remove_dir(DEMO_SERVER_DATA)


def create_dir(path):
    """Create supplied directory"""
    command = ["mkdir", "-p", path]
    subprocess.Popen(command).communicate()

create_dir(DEMO_SERVER_DATA + "/standalone/journal")
create_dir(DEMO_SERVER_DATA + "/replset1a/journal")
create_dir(DEMO_SERVER_DATA + "/replset1b/journal")
create_dir(DEMO_SERVER_DATA + "/replset1c/journal")
create_dir(DEMO_SERVER_DATA + "/shard1a/journal")
create_dir(DEMO_SERVER_DATA + "/shard1b/journal")
create_dir(DEMO_SERVER_DATA + "/config1/journal")
create_dir(DEMO_SERVER_LOG)


def executeCommand(command):
    """Wait a little and then execute shell command"""
    time.sleep(1)
    return subprocess.Popen(command, shell=True)


#========================================= #
#   Helper functions to make sure we move  #
#   on only when we're good and ready      #
#========================================= #


def tryConnection(port):
    """Uses pymongo to try to connect to mongod"""
    error = 0
    try:
        Connection('localhost', port)
    except Exception:
        error = 1
    return error


def checkStarted(port):
    """Checks if our the mongod has started"""
    connected = False

    while not connected:
        error = tryConnection(port)
        if error:
            # Check every 2 seconds
            time.sleep(2)
        else:
            connected = True


#================================= #
#       Run Mongo* processes       #
#================================= #

    
    

class ReplSetManager():

    def startCluster(self):
        # Kill all spawned mongods
        killAllMongoProc()

        # Kill all spawned mongos
        killMongosProc()

        # Create the replica set
        CMD = ["mongod --fork --replSet demo-repl --noprealloc --port 27117 --dbpath "
       + DEMO_SERVER_DATA + "/replset1a --shardsvr --rest --logpath "
       + DEMO_SERVER_LOG + "/replset1a.log --logappend &"]
        executeCommand(CMD)
        checkStarted(27117)

        CMD = ["mongod --fork --replSet demo-repl --noprealloc --port 27118 --dbpath "
       + DEMO_SERVER_DATA + "/replset1b --shardsvr --rest --logpath "
       + DEMO_SERVER_LOG + "/replset1b.log --logappend &"]
        executeCommand(CMD)
        checkStarted(27118)

        CMD = ["mongod --fork --replSet demo-repl --noprealloc --port 27119 --dbpath "
       + DEMO_SERVER_DATA + "/replset1c --shardsvr --rest --logpath "
       + DEMO_SERVER_LOG + "/replset1c.log --logappend &"]
        executeCommand(CMD)
        checkStarted(27119)

# Setup config server
        CMD = ["mongod --oplogSize 500 --fork --configsvr --noprealloc --port 27220 --dbpath " +
        DEMO_SERVER_DATA + "/config1 --rest --logpath "
       + DEMO_SERVER_LOG + "/config1.log --logappend &"]
        executeCommand(CMD)
        checkStarted(27220)

# Setup the mongos
        CMD = ["mongos --port 27217 --fork --configdb localhost:27220 --chunkSize 1  --logpath " 
       + DEMO_SERVER_LOG + "/mongos1.log --logappend &"]
        executeCommand(CMD)
        checkStarted(27217)

# Configure the shards and begin load simulation
        CMD = [SETUP_DIR + "/setup/shell.sh"]
        executeCommand(CMD).wait()

        sys.exit(0)
        



"""
# Create the sharded cluster
CMD = ["mongod --oplogSize 500 --fork --shardsvr --noprealloc --port 27218 "
       "--dbpath "
       + DEMO_SERVER_DATA + "/shard1a --rest --logpath "
       + DEMO_SERVER_LOG + "/shard1a.log --logappend &"]
executeCommand(CMD)
checkStarted(27218)


CMD = ["mongod --oplogSize 500 --fork --shardsvr --noprealloc --port 27219 "
       "--dbpath "
       + DEMO_SERVER_DATA + "/shard1b --rest --logpath "
       + DEMO_SERVER_LOG + "/shard1b.log --logappend &"]
executeCommand(CMD)
checkStarted(27219)

"""





