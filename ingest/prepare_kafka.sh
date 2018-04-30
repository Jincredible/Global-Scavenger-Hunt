#!/bin/bash
# The purpose of this script is to prepare the ingestion cluster to run kafka
# ssh into the master node after starting kafka. then run this script from the home directory of the master node

# the first step is to install kafka-python (later, evaluate if this is needed if we are running the producer in a separate cluster)
pip install kafka-python

# This shell script is created to create a topic in kafka named 'user_data_01'
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic user_data_01

# this command gets all of the topics
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

# this command gets specific details on the topic 'user_data_01'
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic user_data_01