import unittest
import time
from mongo_internal import Daemon
import os

class MongoInternalTester(unittest.TestCase):
    
    def runTest(self):
        unittest.TestCase.__init__(self)    
    

    def test_daemon(self):
        d = Daemon('localhost:27217', 'config.txt', 'http://localhost:8080/solr', ['test.test'], '_id')
       
        d.start()
        
        while len(d.shard_set) != 2:
            time.sleep(2)
        d.stop()
        
        self.assertFalse(d.can_run)
        time.sleep(5)
        for thread in d.shard_set.values():
            self.assertFalse(thread.running)

if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')
    unittest.main()
