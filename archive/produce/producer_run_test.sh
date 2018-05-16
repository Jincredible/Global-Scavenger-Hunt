#!/bin/bash
# The purpose of this script is to prepare the ingestion cluster to run kafka
# ssh into the master node after starting kafka. then run this script from the home directory of the master node
#printf '\n'
printf 'Running producer_run_test.sh:'
#printf '\n'

BASEDIR=$(dirname "$0")
#source ${BASEDIR}/produce_cluster_config.sh #No longer needed. We moved to a config file in python (global_config.py

#python ${BASEDIR}/gps_prod_kafka.py $KAFKA_LISTENER $TOPIC_NAME ${SIM_FILE} #We used this command once when the producer would only generate one user
#python ${BASEDIR}/start_producer.py $KAFKA_LISTENER $TOPIC_NAME #We used this command when the config file was a *.sh file. now moved to global_config.py

python ${BASEDIR}/start_producer.py

# Testing commands:
# Create consumer from console, read from beginning:
# /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic 'user_data_01' --from-beginning

# Create consumer from console, don't read from beginning:
# /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic 'user_data_01'

# Producer from console:
#/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic 'user_data_01'
