#!/bin/bash
bash elasticsearch-configure.sh -f
# $MONGO_HOSTS should be in format HOSTNAME:PORT
# example mongodb01:27017,mongodb02:27017,mongodb03:27017
mongo-connector --auto-commit-interval=0 -m $MONGO_HOSTS -c config/connector.json --stdout
