"""Receives documents from the oplog worker threads and indexes them into the backend. 

This file is a document manager for the Solr search engine, but the intent is that this file can be used as an example to add on different backends. 
"""
#!/usr/env/python
import sys

from pysolr import Solr
from threading import Timer
from util import verify_url, retry_until_ok

class DocManager():
    """The DocManager class creates a connection to the backend engine and adds/removes documents, and in the case of rollback, searches for them. 

    The reason for storing id/doc pairs as opposed to doc's is so that multiple 
    updates to the same doc reflect the most up to date version as opposed to 
    multiple, slightly different versions of a doc.                                                                           
    """
    
    def __init__(self, url, auto_commit = True):
        """Verify Solr URL and establish a connection.
        """
        if verify_url(url) is False:
		    print 'Invalid Solr URL'
		    return None	

        self.solr = Solr(url)
        if auto_commit:
        	self.auto_commit()          


    def upsert(self, doc):
        """Update or insert a document into Solr
        """

        self.solr.add([doc], commit=False)
        
    def remove(self, doc):
        """Removes documents from Solr
        """
        self.solr.delete(id=str(doc['_id']), commit=False)
        
    def search(self, start_ts, end_ts):
        """Called by oplog_manager to query Solr
        """
        query = '_ts: [%s TO %s]' % (start_ts, end_ts)
        
        return self.solr.search(query)
    
    
    def commit(self):
        """This function is used to force a commit.
        
        It is used only in the beginning of rollbacks and in test cases, and is     
        not meant to be called in other circumstances. The body should commit 
        all documents to the backend engine (like auto_commit), but not have any
        timers or run itself again (unlike auto_commit).
        """
        retry_until_ok(self.solr.commit)

    def auto_commit(self):
        """Periodically commits to the Solr server.
        
        This function commits all changes to the Solr engine, and then starts a  
        timer that calls this function again in one second. The reason for this 
        function is to prevent overloading Solr from other searchers. This 
        function may be modified based on the backend engine and how commits are     
        handled, as timers may not be necessary in all    
        instances. 
        """ 
        self.solr.commit()
        Timer(1, self.solr_commit).start() 
        
    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        result = self.solr.search('*:*', sort='_ts desc', rows=1)
        
        if len(result) == 0:
            return None
            
        return result.docs[0]
            
    
            

        
    
        
        
    
    
    
