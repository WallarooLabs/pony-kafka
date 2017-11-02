#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ~

kafka/bin/zookeeper-server-start.sh \
   "${DIR}/zookeeper.properties"
