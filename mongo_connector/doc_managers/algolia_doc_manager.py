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

    def apply_filter(self, doc, tree, filtered_doc):
        exec("filter = self.attributes_filter" + tree )
        if not filter: #todo Remap
            return doc, True

        for key, value in doc.items():
            if key in filter:
                if type(value) != list:
                    value = [value]
                
                _all_ = True if not "_all_" in filter[key] or filter[key]['_all_'] == "and" else False
                _all_op_ = "and" if not "_all_" in filter[key] or filter[key]['_all_'] == "and" else "or"
                for elt in value:
                    if type(elt) == dict:
                        exec('filtered_doc' + tree + '[key] = {}')
                        exec('filtered_doc, state = self.apply_filter(elt, tree + \'["\' + key + \'"]\', filtered_doc)')
                        if not state and _all_op_ == "and":
                            return (filtered_doc, state)
                    else:
                        try:
                            if filter[key] == "" or eval(re.sub(r"_\$", "elt", filter[key])):
                                state = True
                                exec("filtered_doc" + self.remap(tree + '["' + key + '"]') + " = elt")
                            elif _all_op_ == "and":
                                return (filtered_doc, False)
                            else:
                                state = False
                        except Exception, e:
                            logging.warn("Unable to compare during : " + tree + " with : " + key)
                            logging.warn(filter[key])
                            logging.warn(elt)
                            logging.warn(type(elt))
                            logging.warn(e)
                    exec("_all_ = _all_ " + _all_op_ + " state")
        return (filtered_doc, _all_)


    def upsert(self, doc):
        """ Update or insert a document into Algolia
        """
        with self.mutex:
            self.last_object_id = str(doc[self.unique_key]) # mongodb ObjectID is not serializable
            last_update = str(date.today) if not "_ts" in doc else doc['_ts']
            doc, state = self.apply_filter(doc, '', {})
            if not state:
                return
            doc['_ts'] = last_update
            doc[self.unique_key] = doc['objectID'] = self.last_object_id
            print doc
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
            logging.info("nb : " + str(len(self.batch)))
            request = {}
            with self.mutex:
                if len(self.batch) == 0:
                    return
                self.batch.append({ 'action': 'changeSettings', 'body': { 'userData': { 'lastObjectID': self.last_object_id } } })
                self.index.batch({ 'requests': self.batch })
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
        last_object_id = get_last_object_id()
        if last_object_id is None:
            return None
        try:
            return self.index.getObject(last_object_id)
        except algoliasearch.AlgoliaException as e:
            raise errors.ConnectionFailed("Could not connect to Algolia Search: %s" % e)

    def get_last_object_id():
        try:
            return (self.index.getSettings()['userData'] or {}).get('lastObjectID', None)
        except algoliasearch.AlgoliaException as e:
            raise errors.ConnectionFailed("Could not connect to Algolia Search: %s" % e)
