"""A set of utilities used throughout the mongo-connector 
"""

from pymongo import Connection
from bson.timestamp import Timestamp
import httplib
import urlparse

    
def verify_url(url):
    """Verifies the validity of a given url
    """
    host, path = urlparse.urlparse(url)[1:3]
    try:
        conn = httplib.HTTPConnection(host)
        conn.request('HEAD', path)
        return True
    except StandardError:
        return False 
    

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

def retry_until_ok(func, args):
    """Retry code block until it succeeds.
    """

    try:
	func(args)
    except:
	return retry_until_ok(func, args)

    
    
