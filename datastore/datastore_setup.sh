#!/bin/bash

#Objective: set up the datastore cluster
# run this in the datastore cluster, in home directory

BASEDIR=$(dirname "$0")

DATASTORE_PUBLIC_DNS=ubuntu@ec2-34-194-65-102.compute-1.amazonaws.com

POI_FILE_01=${BASEDIR}/../inputs/POI_01.csv
POI_FILE_02=${BASEDIR}/../inputs/POI_02.csv

scp -i ~/.ssh/stevenjin-IAM-keypair.pem ${POI_FILE_01} ${DATASTORE_PUBLIC_DNS}:~/inputs/
scp -i ~/.ssh/stevenjin-IAM-keypair.pem ${POI_FILE_02} ${DATASTORE_PUBLIC_DNS}:~/inputs/

#Start the redis server
#/usr/local/redis/src/redis-server /usr/local/redis/redis.conf

#git clone https://github.com/Jincredible/Global-Scavenger-Hunt
python ${BASEDIR}/import_POI_to_redis.py ${POI_FILE_O1} ${POI_FILE_O2}