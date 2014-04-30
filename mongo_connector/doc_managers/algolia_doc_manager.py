# Copyright 2014 Algolia
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

"""Receives documents from the oplog worker threads and indexes them
    into the backend.

    This file is a document manager for the Algolia search engine.
    """
import logging
import re
import json
from datetime import date
import bson.json_util as bsjson
import bson

from algoliasearch import algoliasearch
from mongo_connector import errors
from threading import Timer, RLock

decoder = json.JSONDecoder()

class DocManager():
    """The DocManager class creates a connection to the Algolia engine and
        adds/removes documents, and in the case of rollback, searches for them.

        Algolia's native 'objectID' field is used to store the unique_key.
        """

    BATCH_SIZE = 1000
    AUTO_COMMIT_DELAY_S = 10

    def __init__(self, url, unique_key='_id', **kwargs):
        """ Establish a connection to Algolia using target url 'APPLICATION_ID:API_KEY:INDEX_NAME'
        """
        application_id, api_key, index = url.split(':')
        self.algolia = algoliasearch.Client(application_id, api_key)
        self.index = self.algolia.initIndex(index)
        self.unique_key = unique_key
        self.last_object_id = None
        self.batch = []
        self.mutex = RLock()
        self.auto_commit = True
        self.run_auto_commit()
        try:
            json = open("algolia_fields_" + index + ".json", 'r')
            self.attributes_filter = decoder.decode(json.read())
            logging.info("Algolia Connector: Start with filter.")
        except IOError: # No filter file
            self.attributes_filter = None
            logging.info("Algolia Connector: Start without filter.")
        try:
            json = open("algolia_remap_" + index + ".json", 'r')
            self.attributes_remap = decoder.decode(json.read())
            logging.info("Algolia Connector: Start with remapper.")
        except IOError: # No filter file
            self.attributes_remap = None
            logging.info("Algolia Connector: Start without remapper.")


    def stop(self):
        """ Stops the instance
        """
        self.auto_commit = False

    def remap(self, tree):
        if self.attributes_remap is None or not tree in self.attributes_remap:
            return tree
        return  self.attributes_remap[tree]

    def serialize(self, value):
        if isinstance(value, bson.objectid.ObjectId):
            return str(value)
        else:
            return value

    def apply_filter(self, doc, filter):
        if not filter:
            return doc, True
        filtered_doc = {}
        _all_root_ = True if not "_all_" in filter or filter['_all_'] == "and" else False
        _all_root_op_ = "and" if not "_all_" in filter or filter['_all_'] == "and" else "or"
        for key, value in doc.items():
            if key in filter:
                if type(value) != list:
                    append = False
                    tabvalue = [value]
                    filtered_doc[key] = {}
                else:
                    tabvalue = value
                    filtered_doc[key] = []
                    append = True
                _all_ = True if not "_all_" in filter[key] or filter[key]['_all_'] == "and" else False
                _all_op_ = "and" if not "_all_" in filter[key] or filter[key]['_all_'] == "and" else "or"
                for elt in tabvalue:
                    if type(elt) == dict:
                        part, state = self.apply_filter(elt, filter[key])
                        if state:
                            if append:
                                filtered_doc[key].append(self.serialize(part))
                            else:
                                filtered_doc[key] = self.serialize(part)
                        elif _all_op_ == "and":
                            del filtered_doc[key]
                            _all_ = False;
                            break;
                    else:
                        try:
                            if filter[key] == "" or eval(re.sub(r"_\$", "elt", filter[key])):
                                state = True
                                if append:
                                    filtered_doc[key].append(self.serialize(elt))
                                else:
                                    filtered_doc[key] = self.serialize(elt)
                            elif _all_op_ == "and":
                                del filtered_doc[key]
                                _all_ = False
                                break;
                            else:
                                state = False
                        except Exception as e:
                            state = True
                            logging.warn("Unable to compare during : " + key)
                            logging.warn(e)
                    exec("_all_ = _all_ " + _all_op_ + " state")
                exec("_all_root_ = _all_ " + _all_root_op_ + " _all_root_")
        return (filtered_doc, _all_root_)

    def apply_remap(self, doc):
        if not self.attributes_remap:
            return doc
        remapped_doc = doc 
        for key, value in self.attributes_remap.items():
            exec("remapped_doc" + value + " = self.serialize(" + "doc" + key + ")")
            exec('del remapped_doc' + key) 
        return remapped_doc
                
    def upsert(self, doc):
        """ Update or insert a document into Algolia
        """
        with self.mutex:
            self.last_object_id = str(doc[self.unique_key]) # mongodb ObjectID is not serializable
            last_update = str(date.today) if not "_ts" in doc else doc['_ts']
            doc, state = self.apply_filter(doc, self.attributes_filter)
            if not state: # delete in case of update
                self.batch.append({ 'action': 'deleteObject', 'body': {'objectID': self.last_object_id } })
                return
            doc = self.apply_remap(doc)
            doc['_ts'] = last_update
            doc[self.unique_key] = doc['objectID'] = self.last_object_id
            self.batch.append({ 'action': 'addObject', 'body': doc })
            if len(self.batch) >= DocManager.BATCH_SIZE:
                self.commit()

    def remove(self, doc):
        """ Removes documents from Algolia
        """
        with self.mutex:
            self.batch.append({ 'action': 'deleteObject', 'body': {"objectID" : str(doc[self.unique_key])} })
            if len(self.batch) >= DocManager.BATCH_SIZE:
                self.commit()

    def search(self, start_ts, end_ts):
        """ Called to query Algolia for documents in a time range.
        """
        try:
            params = {
                numericFilters: '_ts>=%d,_ts<=%d' % (start_ts, end_ts),
                exhaustive: True,
                hitsPerPage: 100000000
            }
            return self.index.search('', params)['hits']
        except algoliasearch.AlgoliaException as e:
            raise errors.ConnectionFailed("Could not connect to Algolia Search: %s" % e)

    def commit(self):
        """ Send the current batch of updates
        """
        try:
            request = {}
            with self.mutex:
                if len(self.batch) == 0:
                    return
                self.index.batch({ 'requests': self.batch })
		self.index.setSettings({ 'userData': { 'lastObjectID': self.last_object_id } })
                self.batch = []
        except algoliasearch.AlgoliaException as e:
            raise errors.ConnectionFailed("Could not connect to Algolia Search: %s" % e)

    def run_auto_commit(self):
        """ Periodically commits to Algolia.
        """
        self.commit()
        if self.auto_commit:
            Timer(DocManager.AUTO_COMMIT_DELAY_S, self.run_auto_commit).start()

    def get_last_doc(self):
        """ Returns the last document stored in Algolia.
        """
        last_object_id = self.get_last_object_id()
        if last_object_id is None:
            return None
        try:
            return self.index.getObject(last_object_id)
        except algoliasearch.AlgoliaException as e:
            raise errors.ConnectionFailed("Could not connect to Algolia Search: %s" % e)

    def get_last_object_id(self):
        try:
            return (self.index.getSettings()['userData'] or {}).get('lastObjectID', None)
        except algoliasearch.AlgoliaException as e:
            raise errors.ConnectionFailed("Could not connect to Algolia Search: %s" % e)
