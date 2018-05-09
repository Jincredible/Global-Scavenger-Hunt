#!/bin/bash

#Start the redis server
#/usr/local/redis/src/redis-server /usr/local/redis/redis.conf &
BASEDIR=$(dirname "$0")
#DATABASE_NUM=7 #this value is 7 if it is a test database and 0 if it is production #We don't have this, moved to global_config file
python ${BASEDIR}/reset_redis.py ${BASEDIR}/POI_01.csv ${BASEDIR}/POI_02.csv 