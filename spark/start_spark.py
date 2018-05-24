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
from pyspark.sql import functions as sqlf

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


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            print(datetime.now(),"================== CREATING NEW SINGLETON =============================")
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class redis_handler(object):
    __metaclass__ = Singleton
    def __init__(self):
        
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


def get_redis_connection():
    if ('redis_connection' not in globals()):
        print(datetime.now(),"================== SETTING CONNECTION =============================")
        pool = redis.ConnectionPool(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
        globals()['redis_connection'] = redis.StrictRedis(connection_pool = pool)
    return globals()['redis_connection']



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
	
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS}) \
                            .map(lambda message: message[1].split(';'))
                            

    DStream_new_users = kafkaStream.filter(lambda message : int(message[4]))
    
    DStream_new_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_new_user_pipe))
    
    DStream_returning_users = kafkaStream.filter(lambda message : not int(message[4]))
    
    DStream_returning_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_returning_user_pipe))


    ssc.start()
    ssc.awaitTermination()
    return





def process_new_user_pipe(iter):
    redis_pipe = redis_handler().connection.pipeline()
    new_users = []
    for record in iter:
        new_users.append(record[0])
        redis_pipe.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(record[2]), latitude=decimal.Decimal(record[3]), radius=200, unit='m')

    candidates = redis_pipe.execute()
    #print('num_users in candidates list:', len(candidates))
    redis_pipe = redis_handler().connection.pipeline()

    for user_index in range(len(candidates)):
        #print('num_target_candidates for user:',new_users[user_index],':', len(candidates[user_index]))
        #num_targets = min([len(candidates[user_index]),config.NUM_LOC_PER_USER])
        num_targets = min([len(candidates[user_index]),config.NUM_LOC_PER_USER])
        #print ('num_targets:', num_targets)
        for target_index in range(num_targets): #range(min(len(candidates[user_index]),config.NUM_LOC_PER_USER))
            #print ('adding user:', new_users[user_index],'target:',candidates[user_index][target_index])
            redis_pipe.sadd(new_users[user_index]+'_targets',candidates[user_index][target_index])

    redis_pipe.execute()
    del new_users
    del candidates
    return



def process_returning_user_pipe(iter):
    redis_pipe = redis_handler().connection.pipeline()
    users = [] 
    for record in iter:
        users.append([record[0],record[2],record[3]]) #username, lon, lat
        redis_pipe.smembers(record[0]+'_targets')

    target_sets = redis_pipe.execute()
    #output of target_sets
    #type: list of sets
    #[{'bos1588', 'bos1675', 'bos1746'},
    #{'bos3721', 'bos3875', 'bos3986'},
    #{'bos1319', 'bos1371', 'bos1400'}]


    #users will need a new column, for an array of targets
    
    for user_index in range(len(users)):
        users[user_index].append([])
        for target in target_sets[user_index]:
            users[user_index][-1].append(target)
            redis_pipe.geopos(config.REDIS_LOCATION_NAME,target)

    target_positions = redis_pipe.execute()
    
    assignments_to_remove = [] #this is a list of [user_index, target_index]
    position_index = 0
    for user_index in range(len(users)):
        for target_index in range(len(users[user_index][3])): #length of 4th column of users table
            target_distance = get_distance(lon_1=decimal.Decimal(users[user_index][1]),lat_1=decimal.Decimal(users[user_index][2]),lon_2=decimal.Decimal(target_positions[position_index][0][0]),lat_2=decimal.Decimal(target_positions[position_index][0][1]))
            
            if target_distance <= config.SCORE_DIST:
                assignments_to_remove.append((user_index,target_index))
                redis_pipe.sadd('removed', users[user_index][3][target_index] + '_' + users[user_index][0] + '_' + str(target_distance))
                redis_pipe.sadd(users[user_index][0] +'_solved',users[user_index][3][target_index])

            position_index += 1

    redis_pipe.execute()

    for assign_index in range(len(assignments_to_remove)):
        user_index = assignments_to_remove[assign_index][0]
        target_index = assignments_to_remove[assign_index][1]
        redis_pipe.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(users[user_index][1]), latitude=decimal.Decimal(users[user_index][2]), radius=500, unit='m')

    candidates = redis_pipe.execute()

    for assign_index in range(len(assignments_to_remove)):
        user_index = assignments_to_remove[assign_index][0]
        redis_pipe.smembers(users[user_index][0] +'_solved')

    solved = redis_pipe.execute()
    
    for assign_index in range(len(assignments_to_remove)):
        user_index = assignments_to_remove[assign_index][0]
        target_index = assignments_to_remove[assign_index][1]
        
        # remove old target
        redis_pipe.srem(users[user_index][0]+'_targets',users[user_index][3][target_index])
        
        #add new target

        candidates_set = set(candidates[assign_index])
        solved_set = set(solved[assign_index])
        current_set = set(users[user_index][3])

        available_set = candidates_set - solved_set - current_set

        users[user_index][3][target_index] = available_set.pop() 
        redis_pipe.sadd(users[user_index][0]+'_targets',users[user_index][3][target_index])
        
        #record it
        redis_pipe.sadd('added',users[user_index][3][target_index] + '_' + users[user_index][0])

    redis_pipe.execute()
    return

# START OF TESTING FUNCTIONS ------------------------------------------------------------------------------
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
    #DStream_new_users = kafkaStream.filter(lambda message : int(message[4]))
    #DStream_new_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_and_iter))
    #DStream_new_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_new_user_pipe))
    
    DStream_returning_users = kafkaStream.filter(lambda message : not int(message[4]))
    #DStream_returning_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_and_iter))
    DStream_returning_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_process_returning_user_pipe))


    ssc.start()
    ssc.awaitTermination()
    return


def test_process_returning_user_pipe(iter):
    redis_pipe = redis_handler().connection.pipeline()
    #redis_pipe = get_redis_connection().pipeline()

    users = [] 
    for record in iter:
        users.append([record[0],record[2],record[3]]) #username, lon, lat
        redis_pipe.smembers(record[0]+'_targets')

    target_sets = redis_pipe.execute()

    #redis-py closes the connection after each execute statement
    redis_pipe = redis_handler().connection.pipeline() 
    #redis_pipe = get_redis_connection().pipeline()
    
    for user_index in range(len(users)):
        users[user_index].append([])
        num_targets = min([len(target_sets[user_index]),config.NUM_LOC_PER_USER])

        for target_index in range(num_targets):
            target = target_sets[user_index].pop()
            users[user_index][-1].append(target)
            redis_pipe.hmget(target,'longitude','latitude')

    target_positions = redis_pipe.execute()
    # result of target_positions using hmget
    # ['-71.073419977183889', '42.380114660229978'],
    # ['-71.03800525348619', '42.371386581176289'],
    # ['-71.025670516818266', '42.374175193811212']]


    #for target in target_sets[user_index]
    #assignments_to_remove = [] #this is a list of [user_index, target_index]
    #position_index = 0
    #for user_index in range(len(users)):
    #    for target_index in range(len(users[user_index][3])): #length of 4th column of users table
    #        target_distance = get_distance(lon_1=decimal.Decimal(users[user_index][1]),lat_1=decimal.Decimal(users[user_index][2]),lon_2=decimal.Decimal(target_positions[position_index][0][0]),lat_2=decimal.Decimal(target_positions[position_index][0][1]))
            
            #if target_distance <= config.SCORE_DIST:
            #    assignments_to_remove.append((user_index,target_index))
            #    redis_pipe.sadd('removed', users[user_index][3][target_index] + '_' + users[user_index][0] + '_' + str(target_distance))
            #    redis_pipe.sadd(users[user_index][0] +'_solved',users[user_index][3][target_index])

    #        position_index += 1

    del target_positions
    return

def process_returning_user(iter):
    redis_connection = redis_handler().connection
    
    for record in iter:
        for target in redis_connection.smembers(record[0]+'_targets'): 
            target_position = redis_connection.geopos(config.REDIS_LOCATION_NAME,target)[0] #geopos returns a list of tuples: [(longitude,latitude)], so to get the tuple out of the list, use [0]
            target_distance = get_distance(lon_1=decimal.Decimal(record[2]),lat_1=decimal.Decimal(record[3]),lon_2=decimal.Decimal(target_position[0]),lat_2=decimal.Decimal(target_position[1]))
            
            #if target_distance <=config.SCORE_DIST:
                #POP target
            #    redis_connection.srem(record[0]+'_targets',target) #EDITED THIS FOR TESTING!! Need to revert later
            #    populate_user_targets_with_redis(redis_connection,record) #EDITED THIS FOR TESTING!! Need to revert later
            #   redis_connection.sadd(record[0]+'_targets',get_candidate_targets_with_redis(redis_connection,record,num_targets=1))
    return


def get_candidate_targets_with_redis(redis_connection,record,out_radius=config.OUTER_RADIUS,in_radius=config.INNER_RADIUS,num_targets=config.NUM_LOC_PER_USER):
    set_outer = set(redis_connection.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(record[2]), latitude=decimal.Decimal(record[3]), radius=out_radius, unit='m'))
    set_inner = set(redis_connection.georadius(name=config.REDIS_LOCATION_NAME, longitude=decimal.Decimal(record[2]), latitude=decimal.Decimal(record[3]), radius=in_radius, unit='m'))
    #also, implement add another set of 'SOLVED' targets for this particular user
    set_targets = set_outer - set_inner
    if (len(set_targets) < num_targets): #and (out_radius <= config.MAX_OUTER_RADIUS)
        return get_candidate_targets_with_redis(redis_connection,record,out_radius+100, in_radius)
    else:
        return set_targets


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
        if 0 < config.NUM_LOC_PER_USER: 
            print('empty function')
        
        for target in range(config.NUM_LOC_PER_USER):
            print('empty function')

            if 50 <= 10:
                print('empty function')
    return

def process_new_user(iter):
# function: add users into redis
    redis_connection = redis_handler().connection
    
    for record in iter:
        possible_target_set = get_candidate_targets_with_redis(redis_connection,record)

        for i in range(config.NUM_LOC_PER_USER):
            redis_connection.sadd(record[0]+'_targets',possible_target_set.pop())

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
        if 0 < config.NUM_LOC_PER_USER: 
            print('empty function')
        
        for target in range(config.NUM_LOC_PER_USER):
            print('empty function')

            if 50 <= 10:
                print('empty function')

    return

# END OF TESTING FUNCTIONS ------------------------------------------------------------------------------

if __name__ == '__main__':

    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")

    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, config.SPARK_MICROBATCH_DURATION)

    
    test_speeds(ssc)
    #main(ssc)


