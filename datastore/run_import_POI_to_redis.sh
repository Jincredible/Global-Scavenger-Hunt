#!/bin/bash

#Start the redis server
#/usr/local/redis/src/redis-server /usr/local/redis/redis.conf &
BASEDIR=$(dirname "$0")
python ${BASEDIR}/import_POI_to_redis.py ${BASEDIR}/POI_01.csv ${BASEDIR}/POI_01.csv