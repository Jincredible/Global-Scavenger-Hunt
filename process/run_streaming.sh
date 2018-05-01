#!/bin/bash
# This is the bash script to submit streaming.py to spark
BASEDIR=$(dirname "$0")
source process_cluster_config.sh
/usr/local/spark/bin/spark-submit --master spark://$process_master_hostname:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --py-files ${BASEDIR}/streaming_config.py ${BASEDIR}/streaming.py


