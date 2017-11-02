#!/bin/bash

cd ~

sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get -y update
sudo sh -c 'echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections'
sudo apt-get -y install oracle-java8-installer

wget http://download.nextag.com/apache/kafka/0.10.2.1/kafka_2.11-0.10.2.1.tgz
tar -xvzf kafka_2.11-0.10.2.1.tgz

mv kafka_2.11-0.10.2.1 kafka

sudo chmod 777 /data

