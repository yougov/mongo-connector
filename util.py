"""A set of utilities used throughout the mongo-connector 
"""

from pymongo import Connection, ReplicaSetConnection, errors

#Name of the oplog collection in a Replica Set configuration
REPL_SET_OPLOG_COLL_NAME = "oplog.rs"


def get_namespace_details(namespace):
    """ Returns the database name and collection name for a given namespace
    """
    db_name, collection_name = namespace.split(".")
    return db_name, collection_name
    
    
def verify_url(url):
    """Verifies the validity of a given url
    """
    ret = True
    try: 
        open(url)
    except IOError: 
        ret = False   
    return ret   

def get_oplog_coll(mongo_conn, mode):
    """Returns the oplog collection for a mongo instance
    """
    oplog_coll = None
    oplog_coll_name = REPL_SET_OPLOG_COLL_NAME
        
    oplog_db = mongo_conn['local']
    oplog_coll = oplog_db[oplog_coll_name]
    
    return oplog_coll
    
def get_connection(address):
    """Takes in an address and returns the mongos connection to it.
    """
    host, port = address.split(':')
    conn = Connection(host, int(port))
    return conn
    
    
def get_next_document(cursor):
    """Gets the next document in the cursor and returns it. 
    """
    doc = None
    for item in cursor:
        doc = item
        break
        
    return doc
    
    
    
        
    
    