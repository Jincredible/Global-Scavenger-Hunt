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

# FOLDER TO MOVE FROM LOCAL TO REMOTE ======================================================================================

LOCAL_FILE_PATH_SIMULATIONS=${BASEDIR}/../kafka/simulations #this is the folder needed to move into each kafka producer instance


# MOVE FOLDER TO THE 3 KAFKA INSTANCES ======================================================================================
#peg scp to-rem ${CLUSTER_NAME_KAFKA} 1 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_KAFKA}
#peg scp to-rem ${CLUSTER_NAME_KAFKA} 2 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_KAFKA}
#peg scp to-rem ${CLUSTER_NAME_KAFKA} 3 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_KAFKA}

scp -i ~/.ssh/stevenjin-IAM-keypair.pem -rp ${LOCAL_FILE_PATH_SIMULATIONS} ubuntu@ec2-52-201-42-228.compute-1.amazonaws.com:~/Global-Scavenger-Hunt/kafka/
scp -i ~/.ssh/stevenjin-IAM-keypair.pem -rp ${LOCAL_FILE_PATH_SIMULATIONS} ubuntu@ec2-18-205-230-98.compute-1.amazonaws.com:~/Global-Scavenger-Hunt/kafka/
scp -i ~/.ssh/stevenjin-IAM-keypair.pem -rp ${LOCAL_FILE_PATH_SIMULATIONS} ubuntu@ec2-34-234-104-182.compute-1.amazonaws.com:~/Global-Scavenger-Hunt/kafka/







