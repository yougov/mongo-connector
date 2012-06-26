"""A set of utilities used throughout the mongo-connector 
"""

from pymongo import Connection
from bson.timestamp import Timestamp

    
def verify_url(url):
    """Verifies the validity of a given url
    """
    ret = True
    try: 
        open(url)
    except IOError: 
        ret = False   
    return ret   
    

def bson_ts_to_long(timestamp):
    """Convert BSON timestamp into integer.
    
    Conversion rule is based from the specs (http://bsonspec.org/#/specification). 
    """
    return ((timestamp.time << 32) + timestamp.inc)
    
    
def long_to_bson_ts(val):
    """Convert integer into BSON timestamp. 
    """
    seconds = val >> 32
    increment = val & 0xffffffff
    
    return Timestamp(seconds, increment)


    
    