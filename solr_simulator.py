class SolrSimulator():
    
    def __init__(self):
        self.doc_dict = {}
        
    
    def upsert(self, doc):
     
        print 'called SolrSimulator upsert'
        print doc
        self.doc_dict[doc['_id']] = doc
        
        
    def remove(self, doc):
    
        del self.doc_dict[doc['_id']]
        
        
    def search(self, start_ts, end_ts):
        
        ret_list = []
        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if ts < end_ts or ts > start_ts:
                ret_list.append(stored_doc)
                
        return ret_list
    
    
    def commit(self): 
        pass


    def auto_commit(self):
        pass 
        
        
    def get_last_doc(self):
        
        last_doc = None
        last_ts = None
        
        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if last_ts is None or ts > last_ts:
                last_doc = stored_doc
                last_ts = ts
        
        return last_doc
             
                
    def test_search(self):
    
        ret_list = []
        for doc in self.doc_dict.values(): 
            ret_list.append(doc)
            
        return ret_list
        
        
    def test_delete(self):
        
        self.doc_dict = {}
            