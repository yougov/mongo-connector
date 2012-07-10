from test_oplog_manager import ReplSetManager
from mongo_internal import Daemon
from pysolr import Solr
from pymongo import Connection

PRIMARY_PORTS = [27117, 27317]

#rsm = ReplSetManager()
#rsm.startCluster()

d = Daemon('localhost:27217', 'config.txt')
d.start()

solr = Solr('http://localhost:8080/solr')
solr.delete(q='*:*')



primary_one = Connection('localhost' , PRIMARY_PORTS[0])
primary_two = Connection('localhost' , PRIMARY_PORTS[1])

primary_one['test']['test'].remove({})
primary_two['test']['best'].remove({})

#general tests to verify that the Daemon is working properly.

primary_one['test']['test'].insert({'name':'paulie'})
primary_two['test']['best'].insert({'name':'jerome'})

results = solr.search('*')
        
assert (len(results) == 2)

results_doc_one = results.docs[0]
results_doc_two = results.docs[1]
assert(results_doc_one['name'] == 'paulie')
assert(results_doc_two['name'] == 'jerome')






    

        




