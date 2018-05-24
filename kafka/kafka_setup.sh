#!/bin/bash

# The purpose of this script is to prepare the ingestion cluster (named kafka) to run kafka
# run this script once after installing zookeeper and kafka

# IN EVERY KAFKA NODE:
# Step 1: install kafka-python, which is the kafka python driver in every node
# pip install kafka-python


# IN KAFKA MASTER NODE:
# this command gets all of the topics
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

# This shell script creates different topics with varying replications and partitions for testing
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic_p1_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic topic_p4_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic topic_p8_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 12 --topic topic_p12_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 16 --topic topic_p16_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 20 --topic topic_p20_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 24 --topic topic_p24_r1
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 32 --topic topic_p32_r1 #workers do not have enough resources to run this

/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic topic_p1_r2
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 4 --topic topic_p4_r2
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 8 --topic topic_p8_r2
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 16 --topic topic_p16_r2

/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic topic_p1_r3
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 4 --topic topic_p4_r3
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 8 --topic topic_p8_r3
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 16 --topic topic_p16_r3




# OLD CODE ====================================

# TOPIC_NAME=user_data_01

# This shell script is created to create a topic in kafka named 'topic_p4_r1'
# /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 18 --topic ${TOPIC_NAME}

#if needed, run this command to change the number of partitions
#/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic ${TOPIC_NAME} --partitions 18

# this command gets specific details on the topic 'topic_p4_r1'
# /usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic ${TOPIC_NAME}

# Testing commands:
# Create consumer from console, read from beginning:
# /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${TOPIC_NAME} --from-beginning

# Create consumer from console, don't read from beginning:
# /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${TOPIC_NAME}

# Producer from console:
#/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${TOPIC_NAME}
