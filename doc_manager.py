"""
Doc_manager.py has a dict that stores all the documents collected by
the oplog workers.

It is meant as an intermediate layer that hides the details of sharding 
and provides an API for backends like Solr/Sphinx/etc to use to gather 
documents. 
"""

class DocManager():
    """
    The DocManager class contains a dictionary that stores id/doc pairs. 

    The reason for storing id/doc pairs as opposed to doc's is so that multiple 
    updates to the same doc reflect the most up to date version as opposed to 
    multiple, slightly different versions of a doc.                                                                           
    """
    
    def __init__(self):
        """
        Just create a dict
        """
        self.doc_dict = {}              
        
        
    def add_doc(self, id, doc):
        """
        Since docs have distinct IDs, there's no overlap.
        
        Thus, we have no need for locking here. Overwrites an existing doc
        if needed. 
        """
        self.doc_dict[id] = doc  
        
        
    def retrieve_docs(self):
        """
        Returns the documents as a list, and clears the dictionary.
        """
        doc_list = self.doc_dict.values()
        self.doc_dict.clear()
        return doc_list
    
    
    