# Author: Asif J
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Splunk implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
Splunk.
"""
import logging

from threading import Timer
import bson.json_util
from mongo_connector import errors
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import retry_until_ok
from mongo_connector.doc_managers import DocManagerBase, exception_wrapper
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter
from bson.json_util import dumps
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import splunklib.client as client
import splunklib.results as results
from time import sleep


try:
    from utils import *
except ImportError:
    raise Exception("Add the SDK repository to your PYTHONPATH to run the examples "
                    "(e.g., export PYTHONPATH=~/splunk-sdk-python.")

wrap_exceptions = exception_wrapper({
    es_exceptions.ConnectionError: errors.ConnectionFailed,
    es_exceptions.TransportError: errors.OperationFailed,
    es_exceptions.RequestError: errors.OperationFailed
})


class DocManager(DocManagerBase):
    """Splunk implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    Splunk.
    """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK,
                 meta_index_name="mongodb_meta", meta_type="mongodb_meta",
                 **kwargs):
        self.host = url[0]
        self.port = url[1] 
        self.username = "admin"
        self.password = "changeme"
        self.auto_commit_interval = auto_commit_interval
        self.doc_type = 'string'  # default type is string, change if needed
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self._formatter = DefaultDocumentFormatter()

    def getConnection(self):
        return client.connect(username=self.username, password=self.password)

    def stop(self):
        """Stop the auto-commit thread."""
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    
    def search(self, start_ts, end_ts):
        """Splunk works on static event which are never get changed, 
           hence no need of taking care of conflicts."""
        tmp = []   
        return tmp;

    def commit(self):
        """Refresh all Splunk indexes."""
        """Splunk does not need any refresh command"""

    def get_last_doc(self):
        """Splunk does not modify any document as it is log analysis tool, hence no documents to be return."""
        return None 

    def update(self, doc, update_spec):
        """Send updated doc to Splunk."""
    
        doc =  dict(doc.items() + update_spec.items())
        index = doc["ns"]
        doc["_time"] = doc["_id"].generation_time

        service = self.getConnection()

        source = index.split(".")
        index_name = index.replace("_","-").replace(".","_").lower()
        # Check index presence
        if index_name not in service.indexes:
            service.indexes.create(index_name)                
        # Index the source document
        index = service.indexes[index_name]
        with index.attached_socket(sourcetype='json', source=source[0], host="abacus") as sock:
            sock.send(dumps(doc, sort_keys=True))  
        print "Updation successful"
        if not doc:
            raise errors.EmptyDocsError(
                "Cannot upsert an empty sequence of "
                "documents into Splunk")      
        return

    def upsert(self, doc):
        """Insert a document into Splunk."""
        index = doc["ns"]
        doc["_time"] = doc["_id"].generation_time
        
        service = self.getConnection()

        source = index.split(".")
        index_name = index.replace("_","-").replace(".","_").lower()
        # Check index presence
        if index_name not in service.indexes:
            service.indexes.create(index_name)                
        # Index the source document
        index = service.indexes[index_name]
        with index.attached_socket(sourcetype='json', source=source[0], host="abacus") as sock:
            sock.send(dumps(doc, sort_keys=True))  
        print "Insertion successful"
        if not doc:
            raise errors.EmptyDocsError(
                "Cannot upsert an empty sequence of "
                "documents into Splunk")      
        return
       
    def bulk_upsert(self, docs):
        """Insert multiple documents into Splunk."""

        for doc in docs:
            index = doc["ns"]            
            doc["_time"] = doc["_id"].generation_time
        
            service = self.getConnection()

            source = index.split(".")
            index_name = index.replace("_","-").replace(".","_").lower()
            # Check index presence
            if index_name not in service.indexes:
                service.indexes.create(index_name)
            # Index the source document               
            index = service.indexes[index_name]
            with index.attached_socket(sourcetype='json', source=source[0], host="abacus") as sock:
                sock.send(dumps(doc, sort_keys=True))                      
    
        if not doc:
            raise errors.EmptyDocsError(
                "Cannot upsert an empty sequence of "
                "documents into Splunk")    
        return  

    def remove(self, doc):
        """Remove a document from Splunk."""      

        index = doc["ns"]
        doc_id = doc["_id"]   

        source = index.split(".")
        index_name = index.replace("_","-").replace(".","_").lower()

        service = self.getConnection()

        jobs = service.jobs
        kwargs_normalsearch = {"exec_mode": "normal"}
        job = jobs.create("search index="+str(index_name)+" id.$oid="+str(doc_id)+" | delete", **kwargs_normalsearch)
        while True:
            job.refresh()
            stats = {"isDone": job["isDone"],
                     "doneProgress": float(job["doneProgress"])*100}
            status = ("\r%(doneProgress)03.1f%%") % stats

            sys.stdout.write(status)
            sys.stdout.flush()
            print "hi"
            if stats["isDone"] == "1":
                sys.stdout.write("\n\nDone!\n\n")
                break
            sleep(1)
        # Get properties of the job
        print "Search job properties"
        print "Search job ID:        ", job["sid"]
        print "Search result: ", job["isDone"]
        print "Search duration:      ", job["runDuration"], "seconds"
        print "This job expires in:  ", job["ttl"], "seconds"
        print "------------------------------------------\n"
        print "Search results:\n"

        print "search index="+str(index_name)+" id.$oid="+str(doc_id)+" | delete"
        # Index the source document               
        index = service.indexes[index_name]
        with index.attached_socket(sourcetype='json', source=source[0], host="abacus") as sock:
            sock.send(dumps(doc, sort_keys=True))                    
        return        