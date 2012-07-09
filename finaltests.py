from clustersetup import ReplSetManager
import time

print 'Preparing cluster'
rsm = ReplSetManager()
rsm.startCluster()
print 'finished startCluster'
rsm.test_retrieve_doc()
print 'passed test_retrieve_doc'
rsm.test_get_oplog_cursor()
print 'passed test_get_oplog_cursor'
rsm.test_get_last_oplog_timestamp()
print 'passed test_get_last_oplog_timestamp'
rsm.test_dump_collection()
print 'passed test_dump_collection'
rsm.test_init_cursor()
print 'passed test_init_cursor'
rsm.test_prepare_for_sync()
print 'passed test_prepare_for_sync'
rsm.test_write_config()
print 'passed test_write_config'
rsm.test_read_config()
print 'passed test_read_config'

print 'passed first round of tests...'
print 'restarting cluster for rollback'
rsm.startCluster()

time.sleep(5)

print 'restarted cluster'
rsm.test_rollback()
print 'passed rollback'
rsm.test_retrieve_doc()
print 'passed test_retrieve_doc'
rsm.test_get_oplog_cursor()
print 'passed test_get_oplog_cursor'
rsm.test_get_last_oplog_timestamp()
print 'passed test_get_last_oplog_timestamp'
rsm.test_dump_collection()
print 'passed test_dump_collection'
rsm.test_init_cursor()
print 'passed test_init_cursor'
rsm.test_prepare_for_sync()
print 'passed test_prepare_for_sync'
rsm.test_write_config()
print 'passed test_write_config'
rsm.test_read_config()
print 'passed test_read_config'

print 'passed all tests!'
