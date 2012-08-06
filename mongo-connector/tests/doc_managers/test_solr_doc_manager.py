# Copyright 2012 10gen, Inc.
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

# This file will be used with PyPi in order to package and distribute the final
# product.

import os
import unittest
import time
import sys
import inspect

file = inspect.getfile(inspect.currentframe())
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))
doc_folder = cmd_folder.rsplit("/", 2)[0]
doc_folder += '/doc_managers'

if doc_folder not in sys.path:
    sys.path.insert(0, doc_folder)

mongo_folder = cmd_folder.rsplit("/", 2)[0]
if mongo_folder not in sys.path:
    sys.path.insert(0, mongo_folder)

from solr_doc_manager import DocManager
from pysolr import Solr

SolrDoc = DocManager("http://localhost:8080/solr/")
solr = Solr("http://localhost:8080/solr/")


class SolrDocManagerTester(unittest.TestCase):
    """Test class for SolrDocManager
    """

    def runTest(self):
        unittest.TestCase.__init__(self)

    def setUp(self):
        """Empty Solr at the start of every test
        """

        solr.delete(q='*:*')

    def test_invalid_URL(self):
        """Ensure DocManager fails for a bad Solr url.
        """
        #Invalid URL
        count = 0
        try:
            s = DocManager("http://doesntexist.cskjdfhskdjfhdsom")
        except SystemError:
            count += 1
        self.assertTrue(count == 1)
        print("PASSED INVALID URL")

    def test_upsert(self):
        """Ensure we can properly insert into Solr via DocManager.
        """
        #test upsert
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert(docc)
        solr.commit()
        res = solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul'}
        SolrDoc.upsert(docc)
        solr.commit()
        res = solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')
        print("PASSED UPSERT")

    def test_remove(self):
        """Ensure we can properly delete from Solr via DocManager.
        """
        #test remove
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert(docc)
        solr.commit()
        res = solr.search('*:*')
        self.assertTrue(len(res) == 1)

        SolrDoc.remove(docc)
        solr.commit()
        res = solr.search('*:*')
        self.assertTrue(len(res) == 0)
        print("PASSED REMOVE")

    def test_full_search(self):
        """Query Solr for all docs via API and via DocManager's _search()
        """
        #test _search
        docc = {'_id': '1', 'name': 'John'}
        SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul'}
        SolrDoc.upsert(docc)
        solr.commit()
        search = SolrDoc._search('*:*')
        search2 = solr.search('*:*')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])
        print("PASSED _SEARCH")

    def test_search(self):
        """Query Solr for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """
        #test search
        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865}
        SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866}
        SolrDoc.upsert(docc)
        docc = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870}
        SolrDoc.upsert(docc)
        solr.commit()
        search = SolrDoc.search(5767301236327972865, 5767301236327972866)
        search2 = solr.search('John')
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        for i in range(0, len(search)):
            self.assertTrue(list(search)[i] == list(search2)[i])
        print("PASSED SEARCH")

    def test_solr_commit(self):
        """Test that documents get properly added to Solr.
        """
        #test solr commit
        docc = {'_id': '3', 'name': 'Waldo'}
        SolrDoc.upsert(docc)
        res = SolrDoc._search('Waldo')
        assert(len(res) == 0)
        time.sleep(2)
        res = SolrDoc._search('Waldo')
        assert(len(res) != 0)
        print("PASSED COMMIT")
        SolrDoc.auto_commit = False

    def test_get_last_doc(self):
        """Insert documents, Verify the doc with the latest timestamp.
        """
        #test get last doc
        docc = {'_id': '4', 'name': 'Hare', '_ts': '2'}
        SolrDoc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': '1'}
        SolrDoc.upsert(docc)
        solr.commit()
        doc = SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')

        docc = {'_id': '6', 'name': 'HareTwin', 'ts': '2'}
        solr.commit()
        doc = SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4' or doc['_id'] == '6')
        print("PASSED GET LAST DOC")

if __name__ == '__main__':
    unittest.main()
