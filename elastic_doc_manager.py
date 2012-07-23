import sys

from pyes import ES, ESRange, RangeQuery, MatchAllQuery
from threading import Timer
from util import verify_url, retry_until_ok
from bson.objectid import ObjectId
import simplejson as json
from util import bson_ts_to_long

class DocManager():

    def __init__ (self, url, auto_commit = True):
        if verify_url(url) is False:
            print 'Invalid ElasticSearch URL'
            return None
        self.elastic = ES(server=url)
        if auto_commit:
            self.auto_commit()

    def upsert(self, doc):
	print doc
        index = doc['ns']
        doc_type = 'string'
        doc['_id'] = str(doc['_id'])
        doc_id = doc['_id']
        self.elastic.index(doc, index, doc_type, doc_id)
	print 'upserted successfully!'

    def remove(self, doc):
        
        self.elastic.delete(doc['ns'], 'string', str(doc['_id']))

    def search(self, start_ts, end_ts):
       res = ESRange('_ts', from_value=start_ts, to_value=end_ts)
       q = RangeQuery(res)
       for it in self.elastic.search(q):
           print it
       results = self.elastic.search(q)
       return results
    
    def _search(self):
        results = self.elastic.search(MatchAllQuery())
        for it in results: print it
        return results

    def commit(self):
        retry_until_ok(self.elastic.refresh)

    def auto_commit(self):
        self.elastic.refresh()
        Timer(1, self.auto_commit).start()

    def get_last_doc(self):
        it = None
        q = MatchAllQuery()
        result = self.elastic.search(q, size=1, sort={'_ts:desc'})
        for it in result: pass
        return it
