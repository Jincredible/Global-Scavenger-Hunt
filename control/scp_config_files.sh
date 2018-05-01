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
CLUSTER_NAME_01=process-cluster
LOCAL_FILE_PATH_01=${SCAVANGER_HOME}/process/streaming_config.py
REMOTE_FILE_PATH_01=./process/
peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 2 ${LOCAL_FILE_PATH_01} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 3 ${LOCAL_FILE_PATH_01} ${REMOTE_FILE_PATH_01}
