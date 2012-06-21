"""
A set of utilities used throughout the mongo-connector project
"""

from pymongo import Connection, ReplicaSetConnection, errors


#Name of the oplog collection in a Replica Set configuration
REPL_SET_OPLOG_COLL_NAME = "oplog.rs"


def get_namespace_details(namespace):
    """ 
    Returns the database name and collection name for a given namespace
    """
    db_name, collection_name = namespace.split(".")
    return db_name, collection_name
    
    
def verify_url(url):
    """
    Verifies the validity of a given url
    """
    ret = True
    try: 
        open(url)
    except IOError: 
        ret = False   
    return ret
    

def upgrade_to_replset(mongo_conn):
    """ 
    Tries to upgrade a normal mongo connection to a replset connection
    """   
     
    stat = mongo_conn['admin'].command({'replSetGetStatus':1})
    
    members = stat['members']
    member_list = ""
    
    for member in members:
        member_list += member['name'] + ','
    
    member_list = member_list[:-1]          # delete trailing comma
    repl_set = stat['set']
        
    try: 
        mongo_conn = ReplicaSetConnection(member_list, replicaSet=repl_set)
    except errors.ConnectionFailure: 
        pass  #do nothing

    return mongo_conn
    

def get_oplog_coll(mongo_conn, mode):
    """
    Returns the oplog collection for a mongo instance, depending on mode (replica set or 
    master/slave).
    """
    
    oplog_coll = None
    if mode == 'repl_set':
        oplog_coll_name = REPL_SET_OPLOG_COLL_NAME
    else:
        oplog_coll_name = MASTER_SLAVE_OPLOG_COLL_NAME
        
    oplog_db = mongo_conn['local']
    #reply = oplog_db.command('collstats', oplog_coll_name, checkResponse=None)
    oplog_coll = oplog_db[oplog_coll_name]
    
    return oplog_coll
    
def get_connection(address):
    """
    For now, it takes in an address and returns the mongos connection to it.
    
    Needs error checking. 
    """
    host, port = address.split(':')
    conn = Connection(host, int(port))
    return conn
    
    
def get_next_document(cursor):
    """
    Returns the next document in the cursor and returns it. 
    """
    doc = None
    for item in cursor:
        doc = item
        break
        
    return doc
    
    
    
        
    
    