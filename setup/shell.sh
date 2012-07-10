#!/bin/bash

LOAD_DIR="$HOME/mongo-connector/mongo-connector/test"
DEMO_SERVER_DATA=$LOAD_DIR/data
DEMO_SERVER_LOG=$LOAD_DIR/logs
SETUP_DIR=$LOAD_DIR/setup
LOAD_SIM_PID_FILE=$LOAD_DIR/load_sim.pid 

#=================================#
#      Configure replica sets     #
#=================================#

mongo localhost:27117  $SETUP_DIR/configReplSet.js

sleep 2

#===================================#
#   Add sharded cluster to mongos   #
#===================================#

echo $SETUP_DIR
mongo --port 27217  $SETUP_DIR/configMongos.js
