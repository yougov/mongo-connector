import sys

from pyelasticsearch import ElasticSearch
from threading import Timer
from util import verify_url, retry_until_ok
from bson.objectid import ObjectId
import simplejson as json
from util import bson_ts_to_long

class DocManager():

    def __init__ (self, url):
        if verify_url(url) is False:
            print 'Invalid ElasticSearch URL'
            return None

        self.elastic = ElasticSearch(url)

    def upsert(self, doc):
        index = doc['ns']
        doc_type = 'string'
        doc_id = doc['_id']
        del doc['ns']
        del doc['_id']
        self.elastic.index(doc, index, doc_type, doc_id)

    def remove(self, doc):
        
        self.elastic.delete(doc['ns'], 'string', str(doc['_id']))

    def search(self, start_ts, end_ts):
        #self.elastic.refresh(['test-index'])
        query = {   "query": { "query_string": { "query": "" }, 
              "filtered": { "filter": { "range": { "_ts": { 
               "from": str(bson_ts_to_long(start_ts)), 
               "to": str(bson_ts_to_long(end_ts)),}, },}, },},}
        ans = self.elastic.search("", body=json.dumps(query), 
              indexes=['test-index'], doc_types=['test-type'])
        results = []
        for doc in ans['hits']['hits']:
            results.append(doc['_source'])
        return results
    
    def _search(self, query):
        results = []
        for doc in self.elastic.search(query)['hits']['hits']:
            results.append(doc['_source'])
        return results
