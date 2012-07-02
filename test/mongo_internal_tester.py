import unittest
from mongo_internal import Daemon

class MongoInternalTester(unittest.TestCase):
    
    def __init__(self):
        super(MongoInternalTester, self).__init__()
        

    def runTest(self):
        d = Daemon('localhost:27000', 'config.txt')
        self.assertTrue(d.isDaemon())
        
        d.stop()
        self.assertFalse(d.canRun)
        for thread in d.shard_set.values():
            self.assertFalse(thread.running)
        