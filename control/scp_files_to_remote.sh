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

CLUSTER_NAME_PRODUCE=produce-cluster
REMOTE_PATH_PRODUCE=./Global-Scavenger-Hunt/produce/

CLUSTER_NAME_PROCESS=process-cluster
REMOTE_PATH_PROCESS=./Global-Scavenger-Hunt/process/

CLUSTER_NAME_REDIS=datastore-single
REMOTE_PATH_REDIS=./Global-Scavenger-Hunt/datastore/

CLUSTER_NAME_CASSANDRA=cassandra-cluster
REMOTE_PATH_CASSANDRA=./Global-Scavenger-Hunt/cassandra/

CLUSTER_NAME_FLASK=flask-single
REMOTE_PATH_FLASK=./Global-Scavenger-Hunt/flask/

# FIRST, MOVE THE GLOBAL CONFIG FILE TO EVERY MASTER NODE --------------------------
LOCAL_FILE_PATH_CONFIG=${BASEDIR}/global_config.py #this config file has all the sensitive info needed for any cluster

peg scp to-rem ${CLUSTER_NAME_PRODUCE} 1 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_PRODUCE}
peg scp to-rem ${CLUSTER_NAME_PROCESS} 1 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_PROCESS}
peg scp to-rem ${CLUSTER_NAME_REDIS} 1 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_REDIS}
peg scp to-rem ${CLUSTER_NAME_CASSANDRA} 1 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_CASSANDRA}
peg scp to-rem ${CLUSTER_NAME_FLASK} 1 ${LOCAL_FILE_PATH_CONFIG} ${REMOTE_PATH_FLASK}





# PROCESS CLUSTER ------------------------------------------------------------------

CLUSTER_NAME_01=process-cluster
REMOTE_FILE_PATH_01=./Global-Scavenger-Hunt/process/

# streaming_config.py file from local to process-cluster
#LOCAL_FILE_PATH_01A=${BASEDIR}/../process/streaming_config.py
#peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01A} ${REMOTE_FILE_PATH_01}

# process_cluster_config.sh from local to process-cluster
LOCAL_FILE_PATH_01B=${BASEDIR}/../process/process_cluster_config.sh
peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01B} ${REMOTE_FILE_PATH_01}

#POI_INPUT files from local into process-cluster
LOCAL_FILE_PATH_01C=${BASEDIR}/../inputs/POI_01.csv
LOCAL_FILE_PATH_01D=${BASEDIR}/../inputs/POI_02.csv
peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01C} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 2 ${LOCAL_FILE_PATH_01C} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 3 ${LOCAL_FILE_PATH_01C} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01D} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 2 ${LOCAL_FILE_PATH_01D} ${REMOTE_FILE_PATH_01}
peg scp to-rem ${CLUSTER_NAME_01} 3 ${LOCAL_FILE_PATH_01D} ${REMOTE_FILE_PATH_01}

#NO LONGER USING REDIS LOCALLY IN PROCESS-CLUSTER
# redis_config.py file from local to process-cluster
#LOCAL_FILE_PATH_01E=${BASEDIR}/../process/redis_config.py
#peg scp to-rem ${CLUSTER_NAME_01} 1 ${LOCAL_FILE_PATH_01E} ${REMOTE_FILE_PATH_01}
#peg scp to-rem ${CLUSTER_NAME_01} 2 ${LOCAL_FILE_PATH_01E} ${REMOTE_FILE_PATH_01}
#peg scp to-rem ${CLUSTER_NAME_01} 3 ${LOCAL_FILE_PATH_01E} ${REMOTE_FILE_PATH_01}

# DATASTORE CLUSTER ----------------------------------------------------------

CLUSTER_NAME_03=datastore-single
REMOTE_FILE_PATH_03=./Global-Scavenger-Hunt/datastore/

#POI_INPUT files from local into datastore
LOCAL_FILE_PATH_03A=${BASEDIR}/../inputs/POI_01.csv
LOCAL_FILE_PATH_03B=${BASEDIR}/../inputs/POI_02.csv
peg scp to-rem ${CLUSTER_NAME_03} 1 ${LOCAL_FILE_PATH_03A} ${REMOTE_FILE_PATH_03}
peg scp to-rem ${CLUSTER_NAME_03} 1 ${LOCAL_FILE_PATH_03B} ${REMOTE_FILE_PATH_03}

#datastore_config.py file from local into datastore
#LOCAL_FILE_PATH_03C=${BASEDIR}/../datastore/datastore_config.py
#peg scp to-rem ${CLUSTER_NAME_03} 1 ${LOCAL_FILE_PATH_03C} ${REMOTE_FILE_PATH_03}

# PRODUCE CLUSTER ----------------------------------------------------------

CLUSTER_NAME_04=produce-cluster
REMOTE_FILE_PATH_04=./Global-Scavenger-Hunt/produce/

# produce_cluster_config.sh from local to produce-cluster
LOCAL_FILE_PATH_04=${BASEDIR}/../produce/produce_cluster_config.sh
peg scp to-rem ${CLUSTER_NAME_04} 1 ${LOCAL_FILE_PATH_04} ${REMOTE_FILE_PATH_04}

# CASSANDRA CLUSTER ----------------------------------------------------------

CLUSTER_NAME_05=cassandra-cluster
REMOTE_FILE_PATH_05=./Global-Scavenger-Hunt/cassandra/

# cassandra_config.py from local to cassandra-cluster
#LOCAL_FILE_PATH_05=${BASEDIR}/../cassandra/cassandra_config.py
#peg scp to-rem ${CLUSTER_NAME_05} 1 ${LOCAL_FILE_PATH_05} ${REMOTE_FILE_PATH_05}







