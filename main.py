"""Temp file that lets you run the system 
"""

from mongo_internal import Daemon
from doc_manager import DocManager
from time import sleep
from pysolr import Solr
from pymongo import Connection
import commands

#commands.getstatusoutput('rm config.txt; touch config.txt')

#solr = Solr('http://127.0.0.1:8080/solr/')
#solr.delete(q="*:*")        #delete everything in solr

dt = Daemon('localhost:27000', 'config.txt')
dt.start()



#commands.getstatusoutput('rm config.txt; touch config.txt')
#solr = Solr('http://127.0.0.1:8080/solr/')
#solr.delete(q="*:*")        #delete everything in solr




    

    
    