import unittest
import time
from mongo_internal import Daemon

class MongoInternalTester(unittest.TestCase):
    
    def __init__(self):
        super(MongoInternalTester, self).__init__()
        

    def runTest(self):
        d = Daemon('localhost:27217', 'config.txt')
        self.assertTrue(d.isDaemon())
        d.start()
        
        while len(d.shard_set) != 2:
            time.sleep(2)
        d.stop()
        
        self.assertFalse(d.canRun)
        for thread in d.shard_set.values():
            self.assertFalse(thread.running)
