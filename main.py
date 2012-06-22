"""Temp file that lets you run the system 
"""

from mongo_internal import DaemonThread
from doc_manager import DocManager
from time import sleep
from pysolr import Solr

dt = DaemonThread('localhost:27000', 'config.txt')
dt.start()

sleep(3)






    

    
