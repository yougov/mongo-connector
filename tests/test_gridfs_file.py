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

import sys

import gridfs

sys.path[0:0] = [""]  # noqa

from mongo_connector import errors
from mongo_connector.gridfs_file import GridFSFile
from mongo_connector.test_utils import ReplicaSetSingle, close_client
from tests import unittest


class TestGridFSFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start up a replica set and connect to it
        cls.repl_set = ReplicaSetSingle().start()
        cls.main_connection = cls.repl_set.client()

    @classmethod
    def tearDownClass(cls):
        close_client(cls.main_connection)
        cls.repl_set.stop()

    def setUp(self):
        # clear existing data
        self.main_connection.drop_database("test")
        self.collection = self.main_connection.test.fs
        self.fs = gridfs.GridFS(self.main_connection.test)

    def get_file(self, doc):
        return GridFSFile(self.collection, doc)

    def test_insert(self):
        def test_insert_file(data, filename, read_size):
            # insert file
            id = self.fs.put(data, filename=filename, encoding="utf8")
            doc = self.collection.files.find_one(id)
            f = self.get_file(doc)

            # test metadata
            self.assertEqual(id, f._id)
            self.assertEqual(filename, f.filename)

            # test data
            result = []
            while True:
                s = f.read(read_size)
                if len(s) > 0:
                    result.append(s.decode("utf8"))
                    if read_size >= 0:
                        self.assertLessEqual(len(s), read_size)
                else:
                    break
            result = "".join(result)
            self.assertEqual(f.length, len(result))
            self.assertEqual(data, result)

        # test with 1-chunk files
        test_insert_file("hello world", "hello.txt", -1)
        test_insert_file("hello world 2", "hello.txt", 10)
        test_insert_file("hello world 3", "hello.txt", 100)

        # test with multiple-chunk files
        size = 4 * 1024 * 1024
        bigger = "".join([chr(ord("a") + (n % 26)) for n in range(size)])
        test_insert_file(bigger, "bigger.txt", -1)
        test_insert_file(bigger, "bigger.txt", 1024)
        test_insert_file(bigger, "bigger.txt", 1024 * 1024)

    def test_missing_chunk(self):
        data = "test data"
        id = self.fs.put(data, encoding="utf8")
        doc = self.collection.files.find_one(id)
        f = self.get_file(doc)

        self.main_connection["test"]["fs.chunks"].delete_one({"files_id": id})

        self.assertRaises(errors.OperationFailed, f.read)


if __name__ == "__main__":
    unittest.main()
