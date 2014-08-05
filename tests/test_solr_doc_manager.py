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

import datetime
import time
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

sys.path[0:0] = [""]

from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.solr_doc_manager import DocManager
from pysolr import Solr
from tests.test_gridfs_file import MockGridFSFile


class SolrDocManagerTester(unittest.TestCase):
    """Test class for SolrDocManager
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the DocManager and a direct connection
        """
        cls.SolrDoc = DocManager("http://localhost:8983/solr/",
                                 auto_commit_interval=0)
        cls.solr = Solr("http://localhost:8983/solr/")

    def setUp(self):
        """Empty Solr at the start of every test
        """

        self.solr.delete(q='*:*')

    def test_update(self):
        doc = {"_id": '1', "ns": "test.test", "_ts": 1,
               "title": "abc", "description": "def"}
        self.SolrDoc.upsert(doc)
        # $set only
        update_spec = {"$set": {"title": "qaz", "description": "wsx"}}
        doc = self.SolrDoc.update(doc, update_spec)
        expected = {"_id": '1', "ns": "test.test", "_ts": 1,
                    "title": "qaz", "description": "wsx"}
        # We can't use assertEqual here, because Solr adds some
        # additional fields like _version_ to all documents
        for k, v in expected.items():
            self.assertEqual(doc[k], v)

        # $unset only
        update_spec = {"$unset": {"title": True}}
        doc = self.SolrDoc.update(doc, update_spec)
        expected = {"_id": '1', "ns": "test.test", "_ts": 1,
                    "description": "wsx"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)
        self.assertNotIn("title", doc)

        # mixed $set/$unset
        update_spec = {"$unset": {"description": True},
                       "$set": {"subject": "edc"}}
        doc = self.SolrDoc.update(doc, update_spec)
        expected = {"_id": '1', "ns": "test.test", "_ts": 1, "subject": "edc"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)
        self.assertNotIn("description", doc)

    def test_upsert(self):
        """Ensure we can properly insert into Solr via DocManager.
        """
        #test upsert
        docc = {'_id': '1', 'name': 'John'}
        self.SolrDoc.upsert(docc)
        res = self.solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul'}
        self.SolrDoc.upsert(docc)
        res = self.solr.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')

    def test_bulk_upsert(self):
        """Ensure we can properly insert many documents at once into
        Solr via DocManager

        """
        self.SolrDoc.bulk_upsert([])

        docs = ({"_id": i, "ns": "test.test"} for i in range(1000))
        self.SolrDoc.bulk_upsert(docs)

        res = sorted(int(x["_id"]) for x in self.solr.search("*:*", rows=1001))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r, i)

        docs = ({"_id": i, "weight": 2*i,
                 "ns": "test.test"} for i in range(1000))
        self.SolrDoc.bulk_upsert(docs)

        res = sorted(int(x["weight"])
                     for x in self.solr.search("*:*", rows=1001))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r, 2*i)

    def test_remove(self):
        """Ensure we can properly delete from Solr via DocManager.
        """
        #test remove
        docc = {'_id': '1', 'name': 'John'}
        self.SolrDoc.upsert(docc)
        res = self.solr.search('*:*')
        self.assertTrue(len(res) == 1)

        self.SolrDoc.remove(docc)
        res = self.solr.search('*:*')
        self.assertTrue(len(res) == 0)

    def test_insert_file(self):
        """Ensure we can properly insert a file into Solr via DocManager.
        """
        test_data = ' '.join(str(x) for x in range(100000))
        docc = {
            '_id': 'test_id',
            '_ts': 10,
            'ns': 'test.ns',
            'filename': 'test_filename',
            'upload_date': datetime.datetime.now(),
            'md5': 'test_md5'
        }
        self.SolrDoc.insert_file(MockGridFSFile(docc, test_data))
        res = self.solr.search('*:*')
        for doc in res:
            self.assertEqual(doc['_id'], docc['_id'])
            self.assertEqual(doc['_ts'], docc['_ts'])
            self.assertEqual(doc['ns'], docc['ns'])
            self.assertEqual(doc['filename'], docc['filename'])
            self.assertEqual(doc['content'][0].strip(),
                             test_data.strip())

    def test_remove_file(self):
        test_data = 'hello world'
        docc = {
            '_id': 'test_id',
            '_ts': 10,
            'ns': 'test.ns',
            'filename': 'test_filename',
            'upload_date': datetime.datetime.now(),
            'md5': 'test_md5'
        }

        self.SolrDoc.insert_file(MockGridFSFile(docc, test_data))
        res = self.solr.search('*:*')
        self.assertEqual(len(res), 1)

        self.SolrDoc.remove(docc)
        res = self.solr.search('*:*')
        self.assertEqual(len(res), 0)

    def test_full_search(self):
        """Query Solr for all docs via API and via DocManager's _search()
        """
        #test _search
        docc = {'_id': '1', 'name': 'John'}
        self.SolrDoc.upsert(docc)
        docc = {'_id': '2', 'name': 'Paul'}
        self.SolrDoc.upsert(docc)
        search = list(self.SolrDoc._search('*:*'))
        search2 = list(self.solr.search('*:*'))
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)
        self.assertTrue(all(x in search for x in search2) and
                        all(y in search2 for y in search))

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
        search = list(self.SolrDoc.search(5767301236327972865,
                                          5767301236327972866))
        search2 = list(self.solr.search('John'))
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)

        result_names = [result.get("name") for result in search]
        self.assertIn('John', result_names)
        self.assertIn('John Paul', result_names)

    def test_solr_commit(self):
        """Test that documents get properly added to Solr.
        """
        docc = {'_id': '3', 'name': 'Waldo', 'ns': 'test.test'}
        docman = DocManager("http://localhost:8983/solr")
        # test cases:
        # -1 = no autocommit
        # 0 = commit immediately
        # x > 0 = commit within x seconds
        for autocommit_interval in [None, 0, 1, 2]:
            docman.auto_commit_interval = autocommit_interval
            docman.upsert(docc)
            if autocommit_interval is None:
                docman.commit()
            else:
                # Allow just a little extra time
                time.sleep(autocommit_interval + 1)
            results = list(docman._search("Waldo"))
            self.assertEqual(len(results), 1,
                             "should commit document with "
                             "auto_commit_interval = %s" % str(
                                 autocommit_interval))
            self.assertEqual(results[0]["name"], "Waldo")
            docman._remove()
            docman.commit()

    def test_get_last_doc(self):
        """Insert documents, Verify the doc with the latest timestamp.
        """
        #test get last doc
        docc = {'_id': '4', 'name': 'Hare', '_ts': '2'}
        self.SolrDoc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': '1'}
        self.SolrDoc.upsert(docc)
        doc = self.SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4')

        docc = {'_id': '6', 'name': 'HareTwin', 'ts': '2'}
        doc = self.SolrDoc.get_last_doc()
        self.assertTrue(doc['_id'] == '4' or doc['_id'] == '6')

    def test_commands(self):
        self.SolrDoc.command_helper = CommandHelper()

        def count_ns(ns):
            return sum(1 for _ in self.SolrDoc._search("ns:%s" % ns))

        self.SolrDoc.upsert({
            '_id': '1',
            'test': 'data',
            'ns': 'test.test',
            'ts': '1'
        })
        self.assertEqual(count_ns("test.test"), 1)

        self.SolrDoc.handle_command({
            'db': 'test',
            'drop': 'test'
        })
        time.sleep(1)
        self.assertEqual(count_ns("test.test"), 0)

        self.SolrDoc.upsert({
            '_id': '2',
            'test': 'data',
            'ns': 'test.test2',
            'ts': '2'
        })
        self.SolrDoc.upsert({
            '_id': '3',
            'test': 'data',
            'ns': 'test.test3',
            'ts': '3'
        })
        self.SolrDoc.handle_command({
            'db': 'test',
            'dropDatabase': 1
        })
        time.sleep(1)
        self.assertEqual(count_ns("test.test2"), 0)
        self.assertEqual(count_ns("test.test3"), 0)


if __name__ == '__main__':
    unittest.main()
