# Copyright 2013-2014 MongoDB, Inc.
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

import os
import time
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
import inspect

sys.path[0:0] = [""]

from mongo_connector.doc_managers.solr_doc_manager import DocManager
from pysolr import Solr, SolrError

class SolrDocManagerTester(unittest.TestCase):
    """Test class for SolrDocManager
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the DocManager and a direct connection
        """
        cls.SolrDoc = DocManager("http://localhost:8983/solr/")
        cls.solr = Solr("http://localhost:8983/solr/")

    def runTest(self):
        """ Runs the tests
        """
        unittest.TestCase.__init__(self)

    def setUp(self):
        """Empty Solr at the start of every test
        """

        self.solr.delete(q='*:*')

    def test_upsert(self):
        """Ensure we can properly insert into Solr via DocManager.
        """
        #test upsert
        docc = {'_id': '1', 'name': 'John'}
        self.SolrDoc.upsert(docc)
        self.solr.commit()
        res = self.solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul'}
        self.SolrDoc.upsert(docc)
        self.solr.commit()
        res = self.solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')

    def test_remove(self):
        """Ensure we can properly delete from Solr via DocManager.
        """
        #test remove
        docc = {'_id': '1', 'name': 'John'}
        self.SolrDoc.upsert(docc)
        self.solr.commit()
        res = self.solr.search('*:*')
        self.assertTrue(len(res) == 1)

        self.SolrDoc.remove(docc)
        self.solr.commit()
        res = self.solr.search('*:*')
        self.assertTrue(len(res) == 0)

    def test_full_search(self):
        """Query Solr for all docs via API and via DocManager's _search()
        """
        #test _search
        docc = {'_id': '1', 'name': 'John'}
        self.SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul'}
        self.SolrDoc.upsert(docc)
        self.solr.commit()
        search = self.SolrDoc._search('*:*')
        search2 = self.solr.search('*:*')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])

    def test_search(self):
        """Query Solr for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """
        #test search
        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865}
        self.SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866}
        self.SolrDoc.upsert(docc)
        docc = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870}
        self.SolrDoc.upsert(docc)
        self.solr.commit()
        search = self.SolrDoc.search(5767301236327972865, 5767301236327972866)
        search2 = self.solr.search('John')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])

    def test_solr_commit(self):
        """Test that documents get properly added to Solr.
        """
        #test solr commit
        docc = {'_id': '3', 'name': 'Waldo'}
        self.SolrDoc.upsert(docc)
        res = self.SolrDoc._search('Waldo')
        assert(len(res) == 1)
        time.sleep(2)
        res = self.SolrDoc._search('Waldo')
        assert(len(res) != 0)
        self.SolrDoc.auto_commit = False

    def test_get_last_doc(self):
        """Insert documents, Verify the doc with the latest timestamp.
        """
        #test get last doc
        docc = {'_id': '4', 'name': 'Hare', '_ts': '2'}
        self.SolrDoc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': '1'}
        self.SolrDoc.upsert(docc)
        self.solr.commit()
        doc = self.SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')

        docc = {'_id': '6', 'name': 'HareTwin', 'ts': '2'}
        self.solr.commit()
        doc = self.SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4' or doc['_id'] == '6')

if __name__ == '__main__':
    unittest.main()
