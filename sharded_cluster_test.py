from test_mongo_internal import *

PORTS_ONE = {"PRIMARY":"27117", "SECONDARY":"27118", "ARBITER":"27119", 
    "CONFIG":"27220", "MONGOS":"27217"}
PORTS_TWO = {"PRIMARY":"27317", "SECONDARY":"27318", "ARBITER":"27319", 
    "CONFIG":"27220", "MONGOS":"27217"}
SETUP_DIR = path.expanduser("~/mongo-connector/test")
DEMO_SERVER_DATA = SETUP_DIR + "/data"
DEMO_SERVER_LOG = SETUP_DIR + "/logs"
MONGOD_KSTR = " --dbpath " + DEMO_SERVER_DATA
MONGOS_KSTR = "mongos --port " + PORTS["CONFIG"]


killAllMongoProc('localhost')
killMongosProc()

remove_dir(DEMO_SERVER_LOG)
remove_dir(DEMO_SERVER_DATA)

create_dir(DEMO_SERVER_DATA + "/standalone/journal")

create_dir(DEMO_SERVER_DATA + "/replset1a/journal")
create_dir(DEMO_SERVER_DATA + "/replset1b/journal")
create_dir(DEMO_SERVER_DATA + "/replset1c/journal")

create_dir(DEMO_SERVER_DATA + "/replset2a/journal")
create_dir(DEMO_SERVER_DATA + "/replset2b/journal")
create_dir(DEMO_SERVER_DATA + "/replset2c/journal")

create_dir(DEMO_SERVER_DATA + "/shard1a/journal")
create_dir(DEMO_SERVER_DATA + "/shard1b/journal")
create_dir(DEMO_SERVER_DATA + "/config1/journal")
create_dir(DEMO_SERVER_LOG)


startMongoProc(PORTS_ONE["PRIMARY"], "demo-repl", "/replset1a", "/replset1a.log")
startMongoProc(PORTS_ONE["SECONDARY"], "demo-repl", "/replset1b", "/replset1b.log")
startMongoProc(PORTS_ONE["ARBITER"], "demo-repl", "/replset1c",
"/replset1c.log")
        
startMongoProc(PORTS_TWO["PRIMARY"], "demo-repl-2", "/replset2a", "/replset2a.log")
startMongoProc(PORTS_TWO["SECONDARY"], "demo-repl-2", "/replset2b", "/replset2b.log")
startMongoProc(PORTS_TWO["ARBITER"], "demo-repl-2", "/replset2c", "/replset2c.log")


# Setup config server
CMD = ["mongod --oplogSize 500 --fork --configsvr --noprealloc --port " + PORTS_ONE["CONFIG"] + " --dbpath " +
DEMO_SERVER_DATA + "/config1 --rest --logpath "
+ DEMO_SERVER_LOG + "/config1.log --logappend &"]
executeCommand(CMD)
checkStarted(int(PORTS_ONE["CONFIG"]))


# Setup the mongos, same mongos for both shards
CMD = ["mongos --port " + PORTS["MONGOS"] + " --fork --configdb localhost:" +    PORTS_ONE["CONFIG"] + " --chunkSize 1  --logpath " 
+ DEMO_SERVER_LOG + "/mongos1.log --logappend &"]

executeCommand(CMD)    
checkStarted(int(PORTS_ONE["MONGOS"]))

# Configure the shards and begin load simulation
cmd1 = "mongo --port " + PORTS_ONE["PRIMARY"] + " " + SETUP_DIR + "/setup/configReplSetSharded1.js"
cmd2 = "mongo --port " + PORTS_TWO["PRIMARY"] + " " + SETUP_DIR + "/setup/configReplSetSharded2.js"
cmd3 = "mongo --port  "+ PORTS["MONGOS"] + " " + SETUP_DIR + "/setup/configMongosSharded.js"

subprocess.call(cmd1, shell=True)
conn = Connection('localhost:' + PORTS_ONE["PRIMARY"])
sec_conn = Connection('localhost:' + PORTS_ONE["SECONDARY"])
        
while conn['admin'].command("isMaster")['ismaster'] is False:
    time.sleep(1)
        
while sec_conn['admin'].command("replSetGetStatus")['myState'] != 2:
    time.sleep(1)
    
subprocess.call(cmd2, shell=True)
conn = Connection('localhost:' + PORTS_TWO["PRIMARY"])
sec_conn = Connection('localhost:' + PORTS_TWO["SECONDARY"])
        
while conn['admin'].command("isMaster")['ismaster'] is False:
    time.sleep(1)
        
while sec_conn['admin'].command("replSetGetStatus")['myState'] != 2:
    time.sleep(1)
    
subprocess.call(cmd3, shell=True)

#cluster of two shards up at this point. 

#test basic functions

primary_one = Connection("localhost:" + PORTS_ONE["PRIMARY"])
primary_two = Connection("localhost:" + PORTS_TWO["PRIMARY"])
    

    

        




