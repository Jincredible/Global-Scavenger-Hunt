#!/bin/bash

#because sensitive files are incorporated into .gitignore file, 
#some of these files will have to be transfered via SCP 

#Objective: SCP config files into clusters whenever updated
# run in the local machine/control machine


# structure of scp command in pegasus
# peg scp <to-local|to-rem|from-local|from-rem> <cluster-name> <node-number> <local-path> <remote-path>
# example
# peg scp to-local judit-spark 1 ./ ./first_try.py

# streaming_config.py file from local to process-cluster
BASEDIR=$(dirname "$0")
CLUSTER_NAME_01=process-cluster
LOCAL_FILE_PATH_01=${BASEDIR}/../process/streaming_config.py
REMOTE_FILE_PATH_01=./Global-Scavenger-Hunt/process/
peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01} ${REMOTE_FILE_PATH_01}
#peg scp to-rem ${CLUSTER_NAME_01} 2 ${LOCAL_FILE_PATH_01} ${REMOTE_FILE_PATH_01}
#peg scp to-rem ${CLUSTER_NAME_01} 3 ${LOCAL_FILE_PATH_01} ${REMOTE_FILE_PATH_01}

# process_cluster_config.sh from local to process-cluster
CLUSTER_NAME_02=process-cluster
LOCAL_FILE_PATH_02=${BASEDIR}/../process/process_cluster_config.sh
REMOTE_FILE_PATH_02=./Global-Scavenger-Hunt/process/
peg scp to-rem ${CLUSTER_NAME_02} 1 ${LOCAL_FILE_PATH_02} ${REMOTE_FILE_PATH_02}

#POI_INPUT files from local into datastore
CLUSTER_NAME_03=datastore-single
LOCAL_FILE_PATH_03A=${BASEDIR}/../inputs/POI_01.csv
LOCAL_FILE_PATH_03B=${BASEDIR}/../inputs/POI_02.csv
REMOTE_FILE_PATH_03=./Global-Scavenger-Hunt/datastore/
peg scp to-rem ${CLUSTER_NAME_03} 1 ${LOCAL_FILE_PATH_03A} ${REMOTE_FILE_PATH_03}
peg scp to-rem ${CLUSTER_NAME_03} 1 ${LOCAL_FILE_PATH_03B} ${REMOTE_FILE_PATH_03}

# produce_cluster_config.sh from local to produce-cluster
CLUSTER_NAME_04=produce-cluster
LOCAL_FILE_PATH_04=${BASEDIR}/../produce/produce_cluster_config.sh
REMOTE_FILE_PATH_04=./Global-Scavenger-Hunt/produce/
peg scp to-rem ${CLUSTER_NAME_04} 1 ${LOCAL_FILE_PATH_04} ${REMOTE_FILE_PATH_04}

