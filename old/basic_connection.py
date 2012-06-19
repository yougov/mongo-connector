  
"""                                                 

class oplogWorker():
    while True:
        addr = addrPool.get()
        if addr == None:
            time.sleep(2)
            continue

        host, port = addr.split(':')
        conn = Connection(host, int(port))
        oplog = conn['local']['oplog']
        
        for row in oplog.rs.find():
            print 'Thread %s printing %s' % self.getName(), row
        addrPool.put(addr)          # restore for the next thread
        time.sleep(5)              # spend 5 secs before checking again



conn = Connection('localhost', 27000)
shards = conn['config']['shards']

#set up queue with all the primary hosts
for shard in shards.find():
    shard_id = shard['_id']
    mongods = shard['host'].split(',')
    for mongod in mongods:
        host = shard['host']
        if '/' in host:
            address = host.split('/')[0]
        else:
            # not a replica set
            address = host
        location, port = address.split(":")
        shard_conn = Connection(location, int(port))
        # upgrade to repl set? - no
        oplog_coll = shard_conn['local']['oplog.rs']
        #start daemon thread for shard
      

    
def begin():
    for i in range(num_workers):
        t = Thread(target=oplogWorker)
        t.daemon = True
        t.setName(str(i));
        t.start()
            
if __name__ == "__main__"
    begin()
    
"""  