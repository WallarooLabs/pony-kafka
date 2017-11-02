#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

sudo rm -r /data/zookeeper
sudo rm -r /data/kafka-logs-0
sudo rm -r /data/kafka-logs-1
sudo rm -r /data/kafka-logs-2

