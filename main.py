"""Temp file that lets you run the system 
"""

from mongo_internal import DaemonThread
from doc_manager import DocManager
from time import sleep

dm = DocManager()
dt = DaemonThread('localhost:27000', dm)
dt.start()

rows = dm.retrieve_docs()

for line in rows:
    print line
    


    

    
