"""
(C) Copyright 2012, 10gen
"""

import subprocess
import sys
import time
from pymongo import Connection
from pymongo.errors import ConnectionFailure
from os import path
from mongo_internal import Daemon
from threading import Timer

# Global path variables
SETUP_DIR = path.expanduser("~/mongo-connector/mongo-connector/test")
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port 27220"
MONGOD_PORTS = ["27117", "27118", "27119", "27218",
                 "27219", "27220", "27017"]

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

def startMongoProc(port, name, data, log):
    #Create the replica set
    CMD = ["mongod --fork --replSet " + name + " --noprealloc --port " + port + " --dbpath "
    + DEMO_SERVER_DATA + data + " --shardsvr --rest --logpath "
    + DEMO_SERVER_LOG + log + " --logappend &"]
    executeCommand(CMD)
    checkStarted(int(port))


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
            #Check every 1 second
            time.sleep(1)
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
        startMongoProc("27117", "demo-repl", "/replset1a", "/replset1a.log")
        startMongoProc("27118", "demo-repl", "/replset1b", "/replset1b.log")
        startMongoProc("27119", "demo-repl", "/replset1c", "/replset1c.log")
        
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
        #executeCommand(CMD).wait()
        cmd1 = "mongo --port 27117 /Users/leostedile/mongo-connector/mongo-connector/test/setup/configReplSet.js"
        cmd2 = "mongo --port 27217 /Users/leostedile/mongo-connector/mongo-connector/test/setup/configMongos.js"
        time.sleep(10)
        subprocess.call(cmd1, shell=True)
        time.sleep(20)
        subprocess.call(cmd2, shell=True)
        #time.sleep(10)
        #subprocess.call(cmd2, shell=True)    
    
    def abort_test(self):
        print 'test failed'
        sys.exit(1)
    
    def testMongoInternal(self):
        t = Timer(60, self.abort_test)
        t.start()
        d = Daemon('localhost:27217', None)
        d.start()
        while len(d.shard_set) == 0:
            pass
        t.cancel()
        d.stop()
        #the Daemon should recognize a single running shard
        assert len(d.shard_set) == 1
        #we want to add several shards
        


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





