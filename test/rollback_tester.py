from mongo_internal import Daemon
from doc_manager import DocManager
from time import sleep
from pysolr import Solr
from pymongo import Connection
import commands

commands.getstatusoutput('rm config.txt; touch config.txt')
solr = Solr('http://127.0.0.1:8080/solr/')
solr.delete(q="*:*")        #delete everything in solr

conn17 = Connection('localhost', 27017)         #assume conn17 is primary
conn18 = Connection('localhost', 27018)

coll17 = conn17['test']['test']
coll18 = conn18['test']['test']


coll17.remove ( { } )                   #wipe the replset
coll17.insert( { 'name':'steve' } )
conn18.admin.command( { 'shutdown':1} ) #kill a secondary

coll17.insert( { 'name':'paulie' } )

print 'kill the daemon, then kill the primary'

sleep(6)                                # time to restart daemon

#restart 18 as new primary
commands.getstatusoutput(' /Users/aayushu/mongo/bin/mongod --replSet s1 --logpath /Users/aayushu/divepython/logs/2.log --dbpath /Users/aayushu/divepython/data/s1-b --port 27018 --fork --shardsvr')

sleep(5)                                 # give it time to be primary

#restart original primary - it should be secondary now
commands.getoutput('/Users/aayushu/mongo/bin/mongod --replSet s1 --logpath /Users/aayushu/divepython/logs/1.log --dbpath /Users/aayushu/divepython/data/s1-a --port 27017 --fork --shardsvr')

print 'restart the daemon now'

#observe rollback

