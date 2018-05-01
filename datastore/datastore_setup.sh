#!/bin/bash

#Objective: set up the datastore cluster
# run this in the datastore cluster, in home directory

BASEDIR=$(dirname "$0")

DATASTORE_PUBLIC_DNS=ec2-18-208-17-22.compute-1.amazonaws.com

POI_FILE_01=${BASEDIR}/../inputs/POI_01.csv
POI_FILE_02=${BASEDIR}/../inputs/POI_02.csv

scp -i ~/.ssh/stevenjin-IAM-keypair.pem ${POI_FILE_01} ubuntu@${DATASTORE_PUBLIC_DNS}:~/inputs/
scp -i ~/.ssh/stevenjin-IAM-keypair.pem ${POI_FILE_02} ubuntu@${DATASTORE_PUBLIC_DNS}:~/inputs/

read -p "Press enter to ssh into the datastore instance:"
ssh -i ~/.ssh/stevenjin-IAM-keypair.pem ubuntu@${DATASTORE_PUBLIC_DNS}

read -p "Press enter to install redis-py:"
#install redis-py
sudo pip install redis

read -p "Press enter to clone git repo:"
git clone https://github.com/Jincredible/Global-Scavenger-Hunt

cd Global-Scavenger-Hunt
#Still need to develop
#need to edit /usr/local/redis/redis.conf to indicate that we're running in standalone mode first, if we are setting this up in one instance
#read -p "Press enter to start the server and run the python script :"
#Start the redis server
#/usr/local/redis/src/redis-server /usr/local/redis/redis.conf &

#in another terminal...
#python ${BASEDIR}/import_POI_to_redis.py ${POI_FILE_O1} ${POI_FILE_O2}