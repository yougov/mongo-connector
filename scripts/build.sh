#!/bin/bash
python setup.py bdist_wheel
tar -zcvf mongo-connector.tar dist/mongo_connector-*.whl
