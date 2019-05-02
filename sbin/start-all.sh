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
NAMENODE_CONFIG_PATH="$PROJECT_DIR/etc/namenode-config.toml"

DATANODE="$PROJECT_DIR/impl/datanode/target/debug/datanode"
DATANODE_CONFIG_PATH="$PROJECT_DIR/etc/datanode-config.toml"

RUST_LOG="debug"

# start namenodes
echo "STARTING NAMENODES:"
while read LINE; do
    # parse line into array
    ARRAY=($LINE)

    echo "    ${ARRAY[1]} ${ARRAY[2]}:${ARRAY[3]}"

    if [ ${ARRAY[2]} == "127.0.0.1" ]
    then
        # start namenode
        $NAMENODE ${ARRAY[1]} ${ARRAY[2]} \
            ${ARRAY[3]} $NAMENODE_CONFIG_PATH \
            > $PROJECT_DIR/log/namenode-${ARRAY[1]}.log 2>&1 &

        echo $! > $PROJECT_DIR/log/namenode-${ARRAY[1]}.pid
    else
        echo "TODO - remote start namenode"
    fi
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
        $DATANODE ${ARRAY[1]} ${ARRAY[2]} \
            ${ARRAY[3]} $DATANODE_CONFIG_PATH \
            > $PROJECT_DIR/log/datanode-${ARRAY[1]}.log 2>&1 &

        echo $! > $PROJECT_DIR/log/datanode-${ARRAY[1]}.pid
    else
        echo "TODO - remote start namenode"
    fi
done < <(grep datanode $HOSTS_PATH)
