from mongo_internal import DaemonThread
from doc_manager import DocManager
from time import sleep

dm = DocManager()
dt = DaemonThread('localhost:27000', dm)
dt.start()
sleep(3)

rows = dm.retrieve_docs()

for line in rows:
    print line
    
    
row2 = dm.retrieve_docs()

if len(row2) == 0:
    print 'gotcha'
    

    

    
