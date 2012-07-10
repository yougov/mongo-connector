from test_oplog_manager import ReplSetManager
from mongo_internal import Daemon
from pysolr import Solr
from pymongo import Connection

MONGOS_PORT = 27217

#rsm = ReplSetManager()
#rsm.startCluster()

solr = Solr('http://localhost:8080/solr')
solr.delete(q='*:*')

mongos = Connection('localhost' , MONGOS_PORT)
d = Daemon('localhost:27217', 'config.txt')
d.start()

primary_one['test']['test'].remove({})
primary_two['test']['best'].remove({})

#general tests to verify that the Daemon is working properly.

mongos['test']['test'].insert({'name':'paulie'})
mongos['test']['best'].insert({'name':'jerome'})

results = solr.search('*')
        
assert (len(results) == 2)

results_doc_one = results.docs[0]
results_doc_two = results.docs[1]
assert(results_doc_one['name'] == 'paulie')
assert(results_doc_two['name'] == 'jerome')






    

        




