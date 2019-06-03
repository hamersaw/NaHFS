#!/bin/bash

# check arguments
if [ $# != 0 ]
then
    echo "usage: $0"
    exit
fi

# initialize instance variables
PROJECT_DIR="$(pwd)/$(dirname $0)/.."
HOSTS_PATH="$PROJECT_DIR/etc/hosts.txt"

# stop namenodes
echo "STOPPING NAMENODES:"
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "    ${ARRAY[1]} ${ARRAY[2]}:${ARRAY[3]}"

    if [ ${ARRAY[2]} == "127.0.0.1" ]
    then
        # stop namenode
        kill `cat $PROJECT_DIR/log/namenode-${ARRAY[1]}.pid`;
        rm $PROJECT_DIR/log/namenode-${ARRAY[1]}.pid
    else
        # stop remote namenode
        ssh rammerd@${ARRAY[2]} -n "kill `cat $PROJECTDIR/log/namenode-${ARRAY[1]}.pid`; \
            rm $PROJECTDIR/log/namenode-${ARRAY[1]}.pid"
    fi
done < <(grep namenode $HOSTS_PATH)

# stop datanodes
echo "STOPPING DATANODES:"
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "    ${ARRAY[1]} ${ARRAY[2]}:${ARRAY[3]}"

    if [ ${ARRAY[2]} == "127.0.0.1" ]
    then
        # stop datanode
        kill `cat $PROJECT_DIR/log/datanode-${ARRAY[1]}.pid`;
        rm $PROJECT_DIR/log/datanode-${ARRAY[1]}.pid
    else
        # stop remote datanode
        ssh rammerd@${ARRAY[2]} -n "kill `cat $PROJECTDIR/log/datanode-${ARRAY[1]}.pid`; \
            rm $PROJECTDIR/log/datanode-${ARRAY[1]}.pid"
    fi
done < <(grep datanode $HOSTS_PATH)
