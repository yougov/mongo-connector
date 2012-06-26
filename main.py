"""Temp file that lets you run the system 
"""

from mongo_internal import Daemon
from doc_manager import DocManager
from time import sleep
from pysolr import Solr
from pymongo import Connection

solr = Solr('http://127.0.0.1:8080/solr/')
solr.delete(q="*:*")        #delete everything in solr

dt = Daemon('localhost:27000', 'config.txt')
dt.start()

conn17 = Connection('localhost', 27017)
conn18 = Connection('localhost', 27018)
conn19 = Connection('localhost', 27019)





    

    
    