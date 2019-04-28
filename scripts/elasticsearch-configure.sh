#!/bin/bash

PROG=$0
# expect these parameters to be set prior to running the script
# ELASTIC_HOST="elasticsearch"
# ELASTIC_PORT="9200"
CONFIG_DIR="./config"
FORCE=0

function usage {
    echo "usage: $PROG [-f] [-h]"
    echo
    echo "clears all Elasticsearch indexes and reconfigures with the definitions in $CONFIG_DIR"
    echo
    echo "connects to $ELASTIC_HOST:$ELASTIC_PORT"
    echo
    echo "    -f    force clear/reconfigure, skips confirmation"
    echo "    -h    display this help text"
    echo
}

while getopts "fh" opt; do
    case $opt in
        f)
            echo "forcing operation / skipping confirmation"
            echo
            FORCE=1
        ;;
        h)
            usage
            exit 0
        ;;
        *)
            usage
            exit
        ;;
    esac
done

if [ $FORCE != 1 ]; then
    echo
    echo "WARNING! THIS WILL DELETE ALL ELASTICSEARCH INDEXES AND DATA AND RECONFIGURE ELASTICSEARCH"
    read -r -p "DO YOU WISH TO PROCEED? [y/N] " CONFIRMATION
    echo
    if [[ "$CONFIRMATION" =~ ^([yY][eE][sS]|[yY])+$ ]]; then
        echo "CONFIRMED - PROCEEDING"
        echo
    else
        echo "NOT CONFIRMED - ABORTING"
        echo
        exit 0
    fi
else
    echo
    echo "FORCE IS TRUE - PROCEEDING"
    echo
fi

##
##  DELETE/RECREATE/RECONFIGURE INDEXES AND MAPPINGS
##
echo "DELETING UNITY INDEX"
curl -XDELETE "$ELASTIC_HOST:$ELASTIC_PORT/unity"
echo
echo
echo "DELETING MONGODB METADATA"
curl -XDELETE "$ELASTIC_HOST:$ELASTIC_PORT/mongodb_meta"
echo
echo

while curl -XGET "$ELASTIC_HOST:$ELASTIC_PORT/unity?pretty" | grep '"status" : 200'; do
    sleep 1
    echo "Waiting for unity index to be removed…"
done

echo
echo "Ok to create unity index"
echo

echo "SETTING UP ELASTICSEARCH INDEX"
##  NOTE: THE FILE settings.json LIMITS TO 1 SHARD AND NO REPLICAS
curl -XPUT "$ELASTIC_HOST:$ELASTIC_PORT/unity/?format=yaml" -d @$CONFIG_DIR/settings.json
echo
echo "ADDING RESOURCETYPES TYPE MAPPING"
curl -XPUT "$ELASTIC_HOST:$ELASTIC_PORT/unity/_mapping/resourceTypes?format=yaml" -d @$CONFIG_DIR/mapping_resourcetypes.json
echo
echo "ADDING PROPERTYTYPES TYPE MAPPING"
curl -XPUT "$ELASTIC_HOST:$ELASTIC_PORT/unity/_mapping/propertyTypes?format=yaml" -d @$CONFIG_DIR/mapping_propertytypes.json
echo
echo "ADDING RESOURCES TYPE MAPPING"
curl -XPUT "$ELASTIC_HOST:$ELASTIC_PORT/unity/_mapping/resources?format=yaml" -d @$CONFIG_DIR/mapping_resources.json

echo
echo "FINISHED"

exit 0
