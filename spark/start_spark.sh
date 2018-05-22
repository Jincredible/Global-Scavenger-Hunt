#!/bin/bash
# This is the bash script to submit streaming.py to spark
BASEDIR=$(dirname "$0")
source ${BASEDIR}/global_config.sh
/usr/local/spark/bin/spark-submit --master spark://$process_master_hostname:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --executor-memory 6G --executor-cores 6 --driver-memory 6G --py-files ${BASEDIR}/$config_filename ${BASEDIR}/$spark_streaming_filename


