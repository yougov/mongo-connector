from test_oplog_manager import ReplSetManager
from mongo_internal import Daemon
from pysolr import Solr
from pymongo import Connection

MONGOS_PORT = 27217
MONGOD_PORTS = [27117, 27317]


solr = Solr('http://localhost:8080/solr')
solr.delete(q='*:*')

rsm = ReplSetManager()
rsm.start_cluster()

mongos = Connection('localhost' , MONGOS_PORT)
"""
primary_one = Connection('localhost', MONGOD_PORTS[0])
primary_two = Connection('localhost', MONGOD_PORTS[1])

primary_one['alpha']['foo'].insert({})      #create those namespaces
primary_two['beta']['foo'].insert({})
"""

mongos['alpha']['foo'].remove({})

d = Daemon('localhost:27217', 'config.txt')
d.start()

#general tests to verify that the Daemon is working properly.

counter = 0
while counter < 18000:                  #enough for two shards`
    mongos['alpha']['foo'].insert({'_id':counter, 'name':'paulie'})
    counter = counter + 1


results = solr.search('*')
        
assert (len(results) == 2)

results_doc_one = results.docs[0]
results_doc_two = results.docs[1]
assert(results_doc_one['name'] == 'paulie')
assert(results_doc_two['name'] == 'jerome')






    

        




