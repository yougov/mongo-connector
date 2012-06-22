"""Stores the documents collected by the oplog workers. 

It is meant as an intermediate layer that hides the details of sharding 
and provides an API for backends like Solr/Sphinx/etc to use to gather 
documents. 
"""

from pysolr import Solr

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


    def upsert(self, docs):
        """Update or insert a document into Solr
        """
        self.solr.add(docs)
        
    
        
        
    
    
    