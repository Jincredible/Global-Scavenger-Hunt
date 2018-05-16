#!/bin/bash

#Objective: set up the datastore cluster
# run this in the datastore cluster, in home directory

read -p "Press enter to install redis-py:"
#install redis-py
sudo pip install redis

read -p "Press enter to clone git repo:"
git clone https://github.com/Jincredible/Global-Scavenger-Hunt

cd Global-Scavenger-Hunt

#Start the redis server
#/usr/local/redis/src/redis-server /usr/local/redis/redis.conf &

#in another terminal...
#python ${BASEDIR}/import_POI_to_redis.py ${POI_FILE_O1} ${POI_FILE_O2}