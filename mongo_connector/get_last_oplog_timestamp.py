#!/usr/bin/python
import pymongo
import sys
from mongo_connector import util

mongo_url = "mongodb://localhost:27017"
if len(sys.argv) == 1:
    print(
        "First argument is mongodb connection string, i.e. "
        "localhost:27017. Assuming localhost:27017..."
    )
if len(sys.argv) >= 2:
    mongo_url = sys.argv[1]


client = pymongo.MongoClient(mongo_url)
rs_name = client.admin.command("ismaster")["setName"]
print("Found Replica Set name: {}".format(str(rs_name)))

print("Now checking for the latest oplog entry...")
oplog = client.local.oplog.rs
last_oplog = oplog.find().sort("$natural", pymongo.DESCENDING).limit(-1).next()
print("Found the last oplog ts: {}".format(str(last_oplog["ts"])))
last_ts = util.bson_ts_to_long(last_oplog["ts"])
out_str = '["{}", {}]'.format(str(rs_name), str(last_ts))

print(
    "Writing all to file oplog.timestamp.last in the format "
    "required for mongo-connector"
)
f = open("./oplog.timestamp.last", "w")
f.write(out_str)
f.close()
print("All done!")
