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
            self.elastic = None
            print 'Invalid ElasticSearch URL'
            return None
        self.elastic = ES(server=url)
	self.auto_commit = auto_commit
        if auto_commit:
            self.run_auto_commit()

    def upsert(self, doc):
        index = doc['ns']
        doc_type = 'string'
        doc['_id'] = str(doc['_id'])
        doc_id = doc['_id']
        self.elastic.index(doc, index, doc_type, doc_id)

    def remove(self, doc):        
        try:
            self.elastic.delete(doc['ns'], 'string', str(doc['_id']))
        except:
            pass

    def _remove(self):
        try:
            self.elastic.delete('test.test', 'string', '')
        except:
            pass

    def search(self, start_ts, end_ts):
       res = ESRange('_ts', from_value=start_ts, to_value=end_ts)
       q = RangeQuery(res)
       results = self.elastic.search(q)
       return results
    
    def _search(self):
        results = self.elastic.search(MatchAllQuery())
        return results

    def commit(self):
        retry_until_ok(self.elastic.refresh)

    def run_auto_commit(self):
        self.elastic.refresh()
	
	if self.auto_commit:
            Timer(1, self.run_auto_commit).start()

    def get_last_doc(self):
        it = None
        q = MatchAllQuery()
        result = self.elastic.search(q, size=1, sort={'_ts:desc'})
        for it in result:
            r = it
            break
        return r
