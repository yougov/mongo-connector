import Queue
import time
import sphinxapi
from threading import Thread
from pymongo import Connection

#initialize Queue that stores addresses i.e. 'host:port' combos
addrPool = Queue.Queue(0)

#set number of workers
num_workers = 2

class SphinxSyncThread(Thread):

    def __init__(self, sphinx):
        """
        Initialize thread and sphinx server
        """
    
        Thread.__init__(self)
        self.thread = None
        self.sphinx = sphinx
        
    def start(self):
        """
        Start thread by calling sphinx server's sync() method.
        """
    
        if self.thread is None:
            self.thread = Thread.start()
            self.sphinx.sync()
    
    def stop(self):
        """
        Stop the thread and stop sphinx (if it's running).
        """
        
        if self.thread is not None:
            self.sphinx.stop()
        
        self.thread.join()
        self.thread = None


class DaemonThread(Thread):
    
    def __init__(self, daemon, host):
        """
        Initialize the daemon thread
        """
    
        Thread.__init__(self)
        self.daemon = daemon
        self.host = host
        self.thread = False
        
    def start(self, *args):
        """ 
        Start the thread, run the daemon. 
        """
    
        if self.thread is None:
            self.thread = Thread.start() 
            self.daemon.run(*args)

    def stop(self):
        """
        Stop the thread and daemon (if it's running). 
        """
        
        if self.thread is not None:
            self.daemon.stop()
            
        self.thread.join()
        self.thread = None
        
        
class Daemon():
    
    def __init__(self):
        """
        Initialize the Daemon.
        """
    
        self.stop = None
        
    def run(mongo_connection, oplog_collection, is_sharded):
        """
        Connect to the external service, and send information. 
        """
        
        sphinx_sync_set = {}
    
        while True:
            new_sphinx_sync_set = {}

            #we will assume that we are passed in the host/port for sphinx servers
            url = "127.0.0.1:9312"

            new_ns_set = {}
            host,port = url.split(":")
            new_ns_set[host] = "port"

            if sphinx_sync_set.has_key(url):
                sphinx_sync = sphinx_sync_set[url]
                sphinx_sync.update_config({'ns_set':new_ns_set})
                
                del sphinx_sync_set[url]  
            elif url_ok?(url):
                sphinx = sphinxapi.SphinxClient()
                sphinx.setServer(host, port)

                sphinx_sync = SphinxSyncThread(new SphinxSynchronizer(sphinx, 
                mongo_connection, oplog_collection, None, new_ns_set, is_sharded)
                sphinx_sync.start()
            else:
                sphinx_sync = nil

            if sphinx_sync is not None:
                new_sphinx_sync_set[url] = sphinx_sync
                    
            #clear out the old config threads    
            for url, sphinx_thread in sphinx_sync_set:
                sphinx_thread.stop()
                
            sphinx_sync_set = new_sphinx_sync_set
            sleep 5 # some interval
            
  
    def run_w_shard(mongo_conn):
        """
        Continuously collect doc information from a sharded cluster. 
        """
        
        shard_set = {}
        shard_coll = mongo_conn['config']['shards']
        
        while True: 
            new_shard_set = {}
            
            for shard_doc in shard_coll.find():
                shard_id = shard_doc['_id']
                host = shard_doc['host'].split(',')[0]
                
                if shard_set.has_key(shard_id):
                    shard = shard_set[shard_id]
                    del shard_set[shard_id]
                elif: '/' in host:
                    address = host.split('/')[0]
                else: # not a replica set
                    address = host
                    
                location,port = address.split(':')
                shard_conn = Connection(location, int(port))
                shard_conn = upgrade_to_replset(shard_conn)
                
                if isinstance(shard_conn, ReplicaSetConnection):
                    oplog_coll = get_oplog_coll(shard_conn, 'repl_set')
                else:
                    oplog_coll = get_oplog_coll(shard_conn, 'master_slave')
                
                shard = DaemonThread.new(Daemon.new(), host)
                shard.start(mongo_conn, oplog_coll, True) #is sharded
                new_shard_set[shard_id] = shard
                
            for id, daemon_thread in shard_set:
                daemon_thread.stop()
                
            shard_set = new_shard_set
            sleep 5 #wait some arbitrary amount of time        
  
  
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