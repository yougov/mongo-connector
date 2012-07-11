#!/usr/bin/env python

"""Temp file that lets you run the system 

Quick start instructions:
1. Set up the ReplicaSet. 
2. Connect to the primary
3. Run main.py
4. Start adding documents to the primary. Confirm changes via Solr web interface.
5. Delete docs to confirm deletion. 
"""

from mongo_internal import Daemon
from optparse import OptionParser

parser = OptionParser()

parser.add_option("-m", "--mongos", action="store", type="string", dest="mongos_addr", 
    default="localhost:27217")
parser.add_option("-o", "--oplog-config", action="store", type="string", dest="oplog_config",
    default="config.txt")
    
(options, args) = parser.parse_args()

dt = Daemon(options.mongos_addr, options.oplog_config)
dt.start()









    

    
    
