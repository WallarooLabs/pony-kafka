#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd ~

kafka/bin/kafka-server-start.sh \
    "${DIR}/kafka-server-2.properties"

