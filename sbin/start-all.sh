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

NAMENODE="$PROJECT_DIR/impl/namenode/target/debug/namenode"
DATANODE="$PROJECT_DIR/impl/datanode/target/debug/datanode"

NAMENODE_IP=""
NAMENODE_PORT=""

# start namenodes
echo "STARTING NAMENODES:"
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "    ${ARRAY[1]} ${ARRAY[2]}:${ARRAY[3]}"

    if [ ${ARRAY[2]} == "127.0.0.1" ]
    then
        # start namenode
        RUST_LOG=debug $NAMENODE ${ARRAY[4]} \
            -i ${ARRAY[2]} -p ${ARRAY[3]} \
                > $PROJECT_DIR/log/namenode-${ARRAY[1]}.log 2>&1 &

        echo $! > $PROJECT_DIR/log/namenode-${ARRAY[1]}.pid
    else
        # start remote namenode
        ssh rammerd@${ARRAY[2]} -n "RUST_LOG=debug $NAMENODE \
            ${ARRAY[4]} -i ${ARRAY[2]} -p ${ARRAY[3]} \
                > $PROJECT_DIR/log/namenode-${ARRAY[1]}.log 2>&1 & \
            echo \$! > $PROJECT_DIR/log/namenode-${ARRAY[1]}.pid"
    fi

    NAMENODE_IP=${ARRAY[2]}
    NAMENODE_PORT=${ARRAY[3]}
done < <(grep namenode $HOSTS_PATH)

# start datanodes
echo "STARTING DATANODES:"
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "    ${ARRAY[1]} ${ARRAY[2]}:${ARRAY[3]}"

    if [ ${ARRAY[2]} == "127.0.0.1" ]
    then
        # start datanode
        RUST_LOG=debug $DATANODE ${ARRAY[1]} ${ARRAY[1]} \
            ${ARRAY[4]} -i ${ARRAY[2]} -p ${ARRAY[3]}  \
            -a $NAMENODE_IP -o $NAMENODE_PORT \
                > $PROJECT_DIR/log/datanode-${ARRAY[1]}.log 2>&1 &

        echo $! > $PROJECT_DIR/log/datanode-${ARRAY[1]}.pid
    else
        # start remote datanode
        ssh rammerd@${ARRAY[2]} -n "RUST_LOG=debug $DATANODE \
            ${ARRAY[1]} ${ARRAY[1]} ${ARRAY[4]} -i ${ARRAY[2]} \
            -p ${ARRAY[3]} -a $NAMENODE_IP -o $NAMENODE_PORT \
                > $PROJECT_DIR/log/datanode-${ARRAY[1]}.log 2>&1 & \
            echo \$! > $PROJECT_DIR/log/namenode-${ARRAY[1]}.pid"
    fi
done < <(grep datanode $HOSTS_PATH)
