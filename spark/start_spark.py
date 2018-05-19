#!/usr/bin/env python
############################################################
# This python script is the main script for spark streaming. 
# Here is the format of the data from kafka:
#
# [user_id; timestamp; longitude; latitude; int(just_logged_in)]
############################################################

import os
# add dependency to use spark with kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

import numpy as np
# Spark
from pyspark import SparkContext
# Spark Streaming
from pyspark.streaming import StreamingContext
# Spark SQL Module
from pyspark.sql.context import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


# Kafka
from pyspark.streaming.kafka import KafkaUtils

# General imports
from math import sin, cos, sqrt, atan2
import json, math, datetime
import decimal
from datetime import datetime

# redis
import redis

#cassandra
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

# custom python configuration file we made to store the kafka parameters
import global_config as config


# Global variables
#REDIS_DATABASE = 7 #7 is for testing, 0 is for production #moved to config file
NUM_LOC_PER_USER = 3 #This is the number of target locations for each user at each time
MIN_LOC_PER_USER = 1 # this is in case there aren't enough target locations within the MAX_OUTER_RADIUS
OUTER_RADIUS = 150 #in meters, this is the outer bound distance to fetch target location
MAX_OUTER_RADIUS = 2000 #in meters, this is the maximum distance to fetch targets
INNER_RADIUS = 50 #in meters, this is the inner bound distance to fetch target location
SCORE_DIST = 10 #in meters, distance a player must be to score the point
#REDIS_LOCATION_NAME='Boston' #moved to config file
#NUM_PARTITIONS = 18 #No longer needed, spark automates this

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class redis_handler(object):
    #__metaclass__ = Singleton
    def __init__(self):
        #print(datetime.now(),"================== INSTANTIATING NEW REDIS HANDLER =============================")
        self.pool = redis.ConnectionPool(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)

    @property
    def connection(self):
        try:
            return self._connection
        except AttributeError:
            self.setConnection()
            return self._connection

    def setConnection(self):
        #self._connection = redis.StrictRedis(connection_pool = self.pool)
        #print(datetime.now(),"================== SETTING CONNECTION =============================")
        self._connection = redis.Redis(connection_pool = self.pool)


# getSqlContextInstance From Spark Streaming Tutorial -----------------------------------------
# http://spark.apache.org/docs/1.3.0/streaming-programming-guide.html#dataframe-and-sql-operations
# Lazily instantiated global instance of SQLContext

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# --------------------------------------------------------------------------------------------

def get_distance(lon_1, lat_1, lon_2, lat_2): #inputs must be in degrees
    
    ''' approximation source: http://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/
    distance(lat, lng, lat0, lng0):
    deglen := 110.25
    x := lat - lat0
    y := (lng - lng0)*cos(lat0)
    return deglen*sqrt(x*x + y*y)
    '''
    #We can use approximations because these distances will be relatively close
    length_degree = 110250 #meters per degree
    lat_diff = lat_2 - lat_1
    long_diff = (lon_2 - lon_1)*decimal.Decimal(math.cos(math.radians(lat_2)))
    distance = length_degree*decimal.Decimal(math.sqrt(lat_diff*lat_diff + long_diff*long_diff))

    return distance


def main():    
	
    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")
    
    ssc = StreamingContext(sc, config.SPARK_MICROBATCH_DURATION)
    
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS}) \
                            .map(lambda message: message[1].split(';'))

    ssc.start()
    ssc.awaitTermination()
    return




def test_speeds(ssc):
    # This method simply tests spark speeds for different functions
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS}) \
                            .map(lambda message: message[1].split(';'))
                            

    # KafkaUtils.createDirectStream({parameters}).map(lambda message: message[1].split(';'))
    # [u'user0000469', u'2077.971822', u'-71.09669095277786', u'42.32523143581117', u'0']
    # [u'user0000473', u'2077.978366', u'-71.11637242815704', u'42.25912890355503', u'0']

    

    # Test speeds of writing DStream obj to file: saves the text file in a folder named the timestamp in the logs folder of the working directory, which for each worker is ~/Global-Scavenger-Hunt/spark,
    # because this is where it is run in the master node
    #kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.saveAsTextFile('logs/'+str(float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f")))))
    #kafkaStream.foreachRDD(test_empty_function_per_rdd)
    
    #kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_empty_function_per_partition))
    #kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_empty_function_per_partition))
    # need to run this function twice to get the effect of running foreachPartition on the new and returning users
    #kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_empty_function_per_partition))

    # Tests speeds of setting up a redis partition per iter, iterating through the partition
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_empty_function_per_iter))
    

    # Tests speeds of setting up a redis partition per partition with a new connection every partition, benchmark against empty function
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_per_partition_StrictRedis))

    # Tests speeds of setting up a redis partition per partition with a connection pool, benchmark against empty function
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_per_partition_ConnectionPool))

    # filters if the element 0 of the split message = 1 (if the just_logged_in boolean = 1)
    DStream_new_users = kafkaStream.filter(lambda message : int(message[4]))
    #DStream_new_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_and_iter))
    DStream_new_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_new_user_pipe))
    
    DStream_returning_users = kafkaStream.filter(lambda message : not int(message[4]))
    #DStream_returning_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_and_iter))
    DStream_returning_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_returning_user_pipe))


    ssc.start()
    ssc.awaitTermination()
    return

def test_empty_function_per_rdd(rdd):
    print('empty rdd function')
    return

def test_empty_function_per_partition(iter):
# function: to test establishing redis connections per partition
    print('empty function')
    return

def test_empty_function_per_iter(iter):
# function: to test establishing redis connections per partition, iterating through all of the records
    for record in iter:
        if 0 < NUM_LOC_PER_USER: 
            print('empty function')
        
        for target in range(NUM_LOC_PER_USER):
            print('empty function')

            if 50 <= 10:
                print('empty function')

    return

def test_redis_connection_per_partition_StrictRedis(iter):
# function: to test establishing redis connections per partition
    redis_connection = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
    return

def test_redis_connection_per_partition_ConnectionPool(iter):
# function: to test establishing redis connections per partition with a connection pool
    redis_connection = redis_handler().connection

    #StrictRedis object automatically releases connection
    #http://redis-py.readthedocs.io/en/latest/_modules/redis/client.html#StrictRedis.execute_command
    return

def test_redis_connection_and_iter(iter):
    redis_connection = redis_handler().connection
    for record in iter:
        if 0 < NUM_LOC_PER_USER: 
            print('empty function')
        
        for target in range(NUM_LOC_PER_USER):
            print('empty function')

            if 50 <= 10:
                print('empty function')
    return

def process_new_user(iter):
# function: add users into redis
    redis_connection = redis_handler().connection
    
    for record in iter:
        possible_target_set = get_candidate_targets_with_redis(redis_connection,record)

        for i in range(NUM_LOC_PER_USER):
            redis_connection.sadd(record[0]+'_targets',possible_target_set.pop())

    return

def process_new_user_pipe(iter):
    redis_pipe = redis_handler().connection.pipeline()

    for record in iter:
        redis_pipe.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(record[2]), latitude=decimal.Decimal(record[3]), radius=300, unit='m')

    candidates = redis_pipe.execute()

    i = 0
    for record in iter:
        candidate_set = set(candidates[i])
        for i in range(NUM_LOC_PER_USER):
            try:
                redis_pipe.sadd(record[0]+'_targets',candidate_set.pop())
            except KeyError:
                pass
        i += 1

    redis_pipe.execute()
    return

def get_candidate_targets_with_redis(redis_connection,record,out_radius=OUTER_RADIUS,in_radius=INNER_RADIUS,num_targets=NUM_LOC_PER_USER):
    set_outer = set(redis_connection.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(record[2]), latitude=decimal.Decimal(record[3]), radius=out_radius, unit='m'))
    set_inner = set(redis_connection.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(record[2]), latitude=decimal.Decimal(record[3]), radius=in_radius, unit='m'))
    #also, implement add another set of 'SOLVED' targets for this particular user
    set_targets = set_outer - set_inner
    if (len(set_targets) < num_targets): #and (out_radius <= MAX_OUTER_RADIUS)
        return get_candidate_targets_with_redis(redis_connection,record,out_radius+100, in_radius)
    else:
        return set_targets

def process_returning_user(iter):
    redis_connection = redis_handler().connection
    
    for record in iter:
        for target in redis_connection.smembers(record[0]+'_targets'): 
            target_position = redis_connection.geopos(config.REDIS_LOCATION_NAME,target)[0] #geopos returns a list of tuples: [(longitude,latitude)], so to get the tuple out of the list, use [0]
            target_distance = get_distance(lon_1=decimal.Decimal(record[2]),lat_1=decimal.Decimal(record[3]),lon_2=decimal.Decimal(target_position[0]),lat_2=decimal.Decimal(target_position[1]))
            
            #if target_distance <=SCORE_DIST:
                #POP target
            #    redis_connection.srem(record[0]+'_targets',target) #EDITED THIS FOR TESTING!! Need to revert later
            #    populate_user_targets_with_redis(redis_connection,record) #EDITED THIS FOR TESTING!! Need to revert later
            #   redis_connection.sadd(record[0]+'_targets',get_candidate_targets_with_redis(redis_connection,record,num_targets=1))

    return

def process_returning_user_pipe(iter):
    redis_pipe = redis_handler().connection.pipeline()
    
    for record in iter:
        redis_pipe.smembers(record[0]+'_targets')

    target_sets = redis_pipe.execute()
    #output of target_sets
    #type: list of sets
    #[{'bos1588', 'bos1675', 'bos1746'},
    #{'bos3721', 'bos3875', 'bos3986'},
    #{'bos1319', 'bos1371', 'bos1400'}]

    for i_set in target_sets:
        set_copy = i_set.copy()
        try:
            redis_pipe.geopos(config.REDIS_LOCATION_NAME,set_copy.pop(),set_copy.pop(),set_copy.pop())
        except KeyError:
            redis_pipe.geopos(config.REDIS_LOCATION_NAME,set_copy.pop())
            pass

    target_positions = redis_pipe.execute()
    #target_positions output:
    #type: list of list of tuples
    #[[(-71.08641654253006, 42.350492466885036),
    #(-71.08616441488266, 42.35092083476096),
    #(-71.08790785074234, 42.35136948040617)],
    #[(-71.07866495847702, 42.333152439433995),
    #(-71.07868105173111, 42.33531202186176),
    #(-71.07865422964096, 42.33414098068614)],
    #[(-71.13807052373886, 42.3527204867841),
    #(-71.13873571157455, 42.35300184083278),
    #(-71.13669723272324, 42.353349097631614)]]

    #target_positions[index_member][index_target][longitude or latitude]
    assignments_to_remove = [] # a list of tuples of (user_id, user_lon, user_lat, target)
    for record in iter:
        index = 0
        for target_coordinates in target_positions[index]:
            target_distance = get_distance(lon_1=decimal.Decimal(record[2]),lat_1=decimal.Decimal(record[3]),lon_2=decimal.Decimal(target_coordinates[0]),lat_2=decimal.Decimal(target_coordinates[1]))
            print('lon:',target_coordinates[0],'lat:',target_coordinates[1],'dist',target_distance)
            if target_distance <= SCORE_DIST:
                assignments_to_remove.append((record[0], record[2], record[3], target))
                
        index += 1

    for assignment in assignments_to_remove:
        redis_pipe.srem(assignment[0] + '_targets', assignment[3])

    redis_pipe.execute()

    for assignment in assignments_to_remove:
        redis_pipe.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(assignment[1]), latitude=decimal.Decimal(assignment[2]), radius=300, unit='m')

    new_candidates = redis_pipe.execute()

    i = 0
    for assignment in assignments_to_remove:
        candidate_set = set(new_candidates[i])
        redis_pipe.sadd(assignment[0]+'_targets',candidate_set.pop())
        i += 1

    redis_pipe.execute()
    return


if __name__ == '__main__':

    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")

    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, config.SPARK_MICROBATCH_DURATION)

    
    test_speeds(ssc)
    #main()


# ------------------ NOTES ----------------------------





