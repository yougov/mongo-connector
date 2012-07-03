#!/bin/bash

LOAD_DIR="$HOME/mongo-connector/mongo-connector/test"
DEMO_SERVER_DATA=$LOAD_DIR/data
DEMO_SERVER_LOG=$LOAD_DIR/logs
SETUP_DIR=$LOAD_DIR/setup
LOAD_SIM_PID_FILE=$LOAD_DIR/load_sim.pid 

#=================================#
#      Configure replica sets     #
#=================================#

mongo localhost:27117 --quiet $SETUP_DIR/configReplSet.js

sleep 10

#===================================#
#   Add sharded cluster to mongos   #
#===================================#

mongo localhost:27217/load_db --quiet $SETUP_DIR/configMongos.js

sleep 10
