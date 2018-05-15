#!/bin/bash

#because sensitive files are incorporated into .gitignore file, 
#some of these files will have to be transfered via SCP 

#Objective: SCP config files into clusters whenever updated
# run in the local machine/control machine


# structure of scp command in pegasus
# peg scp <to-local|to-rem|from-local|from-rem> <cluster-name> <node-number> <local-path> <remote-path>
# example
# peg scp to-local judit-spark 1 ./ ./first_try.py

# TEMP COMMANDS
# produce-cluster 1:~> mkdir TEMP
# produce-cluster 1:~> cp /usr/local/kafka/bin/kafka-topics.sh TEMP
# control-machine:~>peg scp to-local produce-cluster 1 Documents/Insight/Development/Global-Scavenger-Hunt/ingest/ ./TEMP/kafka-topics.sh
# control-machine:~>peg scp to-rem ingest-cluster 1 ./ingest/kafka-topics.sh ./Global-Scavenger-Hunt/ingest
# ingest-cluster 1:~>sudo mv -f ./Global-Scavenger-Hunt/ingest/kafka-topics.sh /usr/local/kafka/bin/

BASEDIR=$(dirname "$0")

# MOVE POINTS OF INTEREST DATAPOINTS TO THE REDIS INSTANCE ======================================================================================
CLUSTER_NAME_REDIS=redis
REMOTE_PATH_REDIS=./Global-Scavenger-Hunt/redis/

LOCAL_FILE_PATH_REDIS_RESET_DATA_A=${BASEDIR}/../redis/POI_01.csv
LOCAL_FILE_PATH_REDIS_RESET_DATA_B=${BASEDIR}/../redis/POI_02.csv

peg scp to-rem ${CLUSTER_NAME_REDIS} 1 ${LOCAL_FILE_PATH_REDIS_RESET_DATA_A} ${REMOTE_PATH_REDIS}
peg scp to-rem ${CLUSTER_NAME_REDIS} 1 ${LOCAL_FILE_PATH_REDIS_RESET_DATA_B} ${REMOTE_PATH_REDIS}






