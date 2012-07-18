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
from sys import exit
from optparse import OptionParser

parser = OptionParser()

#-m is for the mongos address, which is a host:port pair. 
parser.add_option("-m", "--mongos", action="store", type="string", dest="mongos_addr", 
    default="localhost:27217")
    
#-o is to specify the oplog-config file. This file is used by the system to store the last timestamp
#read on a specific oplog. This allows for quick recovery from failure. 
parser.add_option("-o", "--oplog-config", action="store", type="string", dest="oplog_config",
    default="config.txt")

#-b is to specify the URL to the backend engine being used. 
parser.add_option("-b", "--backend-url", action="store", type="string", dest="url", default="")

#-n is to specify the URL of the namespaces we want to consider. The default considers 
#'test.test' and 'alpha.foo'
parser.add_option("-n", "--namespace-set", action="store", type="string", dest="ns_set",
    default="test.test,alpha.foo")

#-u is to specify the uniqueKey used by the backend, 
parser.add_option("-u", "--unique-key", action="store", type="string", dest="u_key", default="_id")
    
(options, args) = parser.parse_args()

try:
    ns_set = options.ns_set.split(',')
except:
    print 'Namespaces must be separated by commas!'
    exit(1)

dt = Daemon(options.mongos_addr, options.oplog_config, options.url, ns_set, options.u_key)
dt.start()









    

    
    
