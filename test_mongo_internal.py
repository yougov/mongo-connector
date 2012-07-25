"""Tests methods from mongo_internal
"""

import unittest
import time
import json
from mongo_internal import Daemon
import os
from setup_cluster import start_cluster
from bson.timestamp import Timestamp
from util import long_to_bson_ts

class MongoInternalTester(unittest.TestCase):
    
    def runTest(self):
        unittest.TestCase.__init__(self)    
    
    def test_daemon(self):
        """Test whether the daemon initiates properly
        """
        
        d = Daemon('localhost:27217', 'config.txt', None, ['test.test'], '_id', None)  
        d.start()
        
        while len(d.shard_set) != 1:
            time.sleep(2)
        d.join()
        
        self.assertFalse(d.can_run)
        time.sleep(5)
        for thread in d.shard_set.values():
            self.assertFalse(thread.running)
            

    def test_write_oplog_progress(self):
        """Test write_oplog_progress under several circumstances
        """
        os.system('touch temp_config.txt')
        config_file_path = os.getcwd() + '/temp_config.txt'
        d = Daemon('localhost:27217', config_file_path, None, ['test.test'], '_id', None)  
        
        #test that None is returned if there is no config file specified.
        self.assertEqual(d.write_oplog_progress(), None)
        
        d.oplog_progress_dict[1] = Timestamp(12,34)           #pretend to insert a thread/timestamp pair
        d.write_oplog_progress()
        
        data = json.load(open(config_file_path, 'r'))
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(12,34))
        
        #ensure the temp file was deleted
        self.assertFalse(os.path.exists(config_file_path + '~'))
        
        #ensure that updates work properly
        d.oplog_progress_dict[1] = Timestamp(44,22)
        d.write_oplog_progress()
        
        data = json.load(open(config_file_path, 'r'))
        self.assertEqual(1, int(data[0]))
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(44,22))
        
        os.system('rm ' + config_file_path)
        print 'PASSED TEST WRITE OPLOG PROGRESS'
        
    def test_read_oplog_progress(self):
        """Test read_oplog_progress
        """
        
        d = Daemon('localhost:27217', None, None, ['test.test'], '_id', None) 
        
        #testing with no file 
        self.assertEqual(d.read_oplog_progress(), None)
        
        os.system('touch temp_config.txt')
        config_file_path = os.getcwd() + '/temp_config.txt'
        d.oplog_checkpoint = config_file_path
        
        #testing with empty file
        self.assertEqual(d.read_oplog_progress(), None)
        
        #add a value to the file, delete the dict, and then read in the value
        d.oplog_progress_dict['oplog1'] = Timestamp(12,34) 
        d.write_oplog_progress()
        del d.oplog_progress_dict['oplog1']
        
        self.assertEqual(len(d.oplog_progress_dict), 0)
        
        d.read_oplog_progress()
        
        self.assertTrue('oplog1' in d.oplog_progress_dict.keys())
        self.assertTrue(d.oplog_progress_dict['oplog1'], Timestamp(12,34))
        
        d.oplog_progress_dict['oplog1'] = Timestamp(55,11) 
        
        #see if oplog progress dict is properly updated
        d.read_oplog_progress()
        self.assertTrue(d.oplog_progress_dict['oplog1'], Timestamp(55,11) )
        
        os.system('rm ' + config_file_path)   
        print 'PASSED TEST READ OPLOG PROGRESS'      

if __name__ == '__main__':
    start_cluster()
    os.system('rm config.txt; touch config.txt')
    unittest.main()
