"""A class to serve as proxy for the Solr engine for testing.
"""

from bson.objectid import ObjectId

class BackendSimulator():
    """BackendSimulator emulates both a Solr DocManager and a Solr server.
    """
    
    def __init__(self):
        """Creates a dictionary to hold document id keys mapped to the documents as values. 
        """
        self.doc_dict = {}
        
    
    def upsert(self, doc):
        """Adds a document to the doc dict.
        """
        self.doc_dict[doc['_id']] = doc
        
        
    def remove(self, doc):
        """Removes the document from the doc dict. 
        """
        del self.doc_dict[doc['_id']]
        
        
    def search(self, start_ts, end_ts):
        """Searches through all documents and finds all documents within the range. 
        
        Since we have very few documents in the doc dict when this is called, linear search is fine. 
        """
        ret_list = []
        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if ts <= end_ts or ts >= start_ts:
                ret_list.append(stored_doc)
                
        return ret_list
    
    
    def commit(self): 
        """Simply passes since we're not using Solr.  
        
        This function exists only because the DocManager is set to BackendSimulator, and the rollback
        function calles doc_manager.commit(), so we have a dummy here. 
        """
        pass
          
    def get_last_doc(self):
        """Searches through the doc dict to find the document with the latest timestamp.
        """
        
        last_doc = None
        last_ts = None
        
        for stored_doc in self.doc_dict.values():
            ts = stored_doc['_ts']
            if last_ts is None or ts >= last_ts:
                last_doc = stored_doc
                last_ts = ts
        
        return last_doc
             
                
    def test_search(self):
        """Proxies Solr.search('*') i.e. returns all documents in the doc dict.
        """
    
        ret_list = []
        for doc in self.doc_dict.values(): 
            ret_list.append(doc)
            
        return ret_list
        
        
    def test_delete(self):
        """Proxies Solr.delete(q='*:*') i.e. deletes all documents. 
        """
        self.doc_dict = {}
            