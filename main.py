"""Temp file that lets you run the system 

Quick start instructions:
1. Set up the ReplicaSet. 
2. Connect to the primary
3. Run main.py
4. Start adding documents to the primary. Confirm changes via Solr web interface.
5. Delete docs to confirm deletion. 
"""

from mongo_internal import Daemon


dt = Daemon('localhost:27000', 'config.txt')
dt.run()








    

    
    
