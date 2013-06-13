cd ..
cp ../doc_managers/doc_manager_simulator.py ../doc_manager.py
python test_mongo_connector.py
python test_mongo_connector.py -m 27117
python test_oplog_manager.py
python test_oplog_manager.py -m 27117
python test_oplog_manager_sharded.py
python test_synchronizer.py
python test_synchronizer.py -m 27117
python test_util.py
python clean_up.py
