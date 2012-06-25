"""Stores the documents collected by the oplog workers. 

It is meant as an intermediate layer that hides the details of sharding 
and provides an API for backends like Solr/Sphinx/etc to use to gather 
documents. 
"""

from pysolr import Solr
from threading import Timer

class SolrDocManager():
    """The DocManager class contains a dictionary that stores id/doc pairs. 

    The reason for storing id/doc pairs as opposed to doc's is so that multiple 
    updates to the same doc reflect the most up to date version as opposed to 
    multiple, slightly different versions of a doc.                                                                           
    """
    
    def __init__(self, url):
        """Just create a dict
        """
        self.solr = Solr(url)   
        self.solr_commit()          


    def upsert(self, docs):
        """Update or insert documents into Solr
        """
        self.solr.add(docs, commit=False)
        
    def remove(self, doc_id):
        """Removes documents from Solr
        """
        self.solr.delete(id=str(doc_id), commit=False)
           
    def solr_commit(self):
        """Periodically commits to the Solr server.
        """ 
        self.solr.commit()
        Timer(10, self.solr_commit).start() 
            

        
    
        
        
    
    
    