from test_oplog_manager import ReplSetManager
from mongo_internal import Daemon
from pysolr import Solr
from pymongo import Connection

MONGOS_PORT = 27217
MONGOD_PORTS = [27117, 27317]

rsm = ReplSetManager()
rsm.start_cluster()

from test_oplog_manager import ReplSetManager
from mongo_internal import Daemon
from pysolr import Solr
from pymongo import Connection

MONGOS_PORT = 27217
MONGOD_PORTS = [27117, 27317]

solr = Solr('http://localhost:8080/solr')
solr.delete(q='*:*')

mongos = Connection('localhost' , MONGOS_PORT)
"""
primary_one = Connection('localhost', MONGOD_PORTS[0])
primary_two = Connection('localhost', MONGOD_PORTS[1])

primary_one['alpha']['foo'].insert({})      #create those namespaces
primary_two['beta']['foo'].insert({})
"""

mongos['test']['test'].remove({})

d = Daemon('localhost:27217', 'config.txt')
d.start()

#general tests to verify that the Daemon is working properly.

n = 10
while n > 0:
    mongos['test']['test'].insert({'name':'paulie'})
    n = n - 1


results = solr.search('*')
        
assert (len(results) == 2)

results_doc_one = results.docs[0]
results_doc_two = results.docs[1]
assert(results_doc_one['name'] == 'paulie')
assert(results_doc_two['name'] == 'jerome')






    

        




