#!/bin/bash
# The purpose of this script is to prepare the ingestion cluster to run kafka
# ssh into the master node after starting kafka. then run this script from the home directory of the master node

printf '\npreparing ingestion cluster. The Objectives are the following:'
printf '\n1. Accept data from producer cluster with a topic'
printf '\n2. Stream all messages in the topic for the processing cluster'
printf '\n'

TOPIC_NAME=user_data_01

# the first step is to install kafka-python (later, evaluate if this is needed if we are running the producer in a separate cluster)
#pip install kafka-python

# This shell script is created to create a topic in kafka named 'user_data_01'
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic ${TOPIC_NAME}

# this command gets all of the topics
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

# this command gets specific details on the topic 'user_data_01'
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic ${TOPIC_NAME}

# Testing commands:
# Create consumer from console, read from beginning:
# /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${TOPIC_NAME} --from-beginning

# Create consumer from console, don't read from beginning:
# /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${TOPIC_NAME}

# Producer from console:
#/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${TOPIC_NAME}
