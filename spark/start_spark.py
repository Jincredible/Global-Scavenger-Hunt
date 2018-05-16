#!/usr/bin/env python
############################################################
# This python script is the main script for spark streaming. 
# Here is the format of the data from kafka:
#
# [user_id; timestamp; longitude; latitude; int(just_logged_in)]
# The "acc" column is the acceleration of the user.
#
# The main tasks of thise script is the following:
#
# 1. Receive streaming data from kafka as a Dstream object 
# 2. Take the original Dstream, get only the latest GPS message of each user
# 3. Does the user exist in the redis database? 
# 3a If Not: fetch N target locations for the user and store them into the
#    user database
# 3b If Yes: next step
# 4. Calculate the distance between the user and each of the user locations.
# 5. Is this distance between the user and a target location less than Y meters?
# 5a If Yes: add the 

#
# The parameters in streaming_config
# streaming_config.KAFKA_TOPIC: name of kafka topic for upstream queue
# streaming_config.KAFKA_DNS: public DNS and port for Kafka messages
# streaming_config.REDIS_DNS: public DNS for Redis instance
# streaming_config.REDIS_PORT: public port for Redis instance
# streaming_config.REDIS_PASS: password for redis authentication
# streaming_config.CASSANDRA_DNS: public DNS of cassandra seed
# streaming_config.CASSANDRA_NAMESPACE: namespace for cassandra
# were written in a separate "streaming-config.py".
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


class redis_handler(object): #this is a metaclass

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

    ''' Haversine formula
    #first, convert to radians
    lon_1 = math.radians(lon_1)
    lon_2 = math.radians(lon_2)
    lat_1 = math.radians(lat_1)
    lat_2 = math.radians(lat_2)

    lon_diff = lon_2 - lon_1
    lat_diff = lat_2 - lat_1
    a = (sin(lat_diff/2))**2 + cos(lat_1) * cos(lat_2) * (sin(lon_diff/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = 6373.0 * c
    '''
    return distance


# ========================== TO REMOVE ===================================

def redis_get_new_targets(r,row,out_radius=OUTER_RADIUS,in_radius=INNER_RADIUS): #returns a set of possible locations
    set_outer = set(r.georadius(name=config.REDIS_LOCATION_NAME, longitude=row.longitude, latitude=row.latitude, radius=out_radius, unit='m'))
    set_inner = set(r.georadius(name=config.REDIS_LOCATION_NAME, longitude=row.longitude, latitude=row.latitude, radius=in_radius, unit='m'))
    #also, implement add another set of 'SOLVED' targets for this particular user
    set_targets = set_outer - set_inner
    if (len(set_targets) < MIN_LOC_PER_USER) or (out_radius >= MAX_OUTER_RADIUS):
        return redis_get_new_targets(r,row,out_radius+100, in_radius)
    else:
        return set_targets

# ========================== TO REMOVE ===================================

def redis_populate_targets(r,c,row):
    #r is the redis handler
    #c is the cassandra handler
    #row is the row object from the spark dataframe object

    possible_target_set = redis_get_new_targets(r,row)
    print('fetched a possible set of',len(possible_target_set),'target locations')

    #We need to add a check to make sure that the possible_target_set has one or more targets in it!

    #prepare query for cassandra insert
    query_add = "INSERT INTO user_target (user_id, target_id, timestamp_produced, timestamp_spark, transaction_type) VALUES (?,?,?,?,?);"
    #for this cassandra table (user_target), it's important to remember that a transaction_type is 1 if we add a target and -1 if we remove it
    
    print ('r.scard(row.userid)', r.scard(row.userid))
    print ('NUM_LOC_PER_USER: ', NUM_LOC_PER_USER)
    while (r.scard(row.userid) < NUM_LOC_PER_USER) and (len(possible_target_set)>0):
        new_target = possible_target_set.pop()
        print('adding member:', new_target,'to user:',row.userid)
        r.sadd(row.userid,new_target)
        print(query_add)
        print('userid:',row.userid)
        print('targetid:',new_target)
        print('time:', long(row.time))
        c.execute(c.prepare(query_add), (row.userid, new_target, long(row.time),long(datetime.now().strftime("%H%M%S%f")), 1))


# ========================== TO REMOVE ===================================

def process_row_redis(row):
    #processes the row, writes to redis

    # 1. can't pass any additional parameters to the foreach commands,
    # 2. encountered difficulty passing the redis handler in lambda. This is because we can't really broadcast this handler
    r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS) #OBVIOUSLY GOING TO BE A BOTTLENECK
    #cluster = Cluster(config.CASSANDRA_DNS)
    #in this case, c is session (session=cluster.connect(<namespace>))
    c = Cluster(config.CASSANDRA_DNS).connect(config.CASSANDRA_NAMESPACE)

    # Debugging: Print to stderr if this doesn't exist
    if r.exists(row.userid): #does the user exist in the database?
        print('user:',row.userid,'does exist in database')
        print('user targets:',r.smembers(row.userid))
    else:
        print('user:',row.userid,'does NOT exist in database')

    #if the user JUST logged and he/she already has members saved, remove those members
    if r.exists(row.userid) and int(row.just_logged_in):
        print('user just logged in and has targets from previous app session')
        print('row.just_logged_in: ',row.just_logged_in)
        while r.scard(row.userid) >0:
            r.spop(row.userid)

    if r.scard(row.userid) < NUM_LOC_PER_USER:
        redis_populate_targets(r,c,row)



    #now that the user has his/her targets, we're goint to populate the correct cassandra database
    query_location = "INSERT INTO user_location (user_id,timestamp_produced, timestamp_spark,longitude,latitude) VALUES (?,?,?,?,?);"
    print(query_location)

    print('userid:',row.userid)
    print('time:', long(row.time))
    print('lon:', decimal.Decimal(row.longitude))
    print('lat:', decimal.Decimal(row.latitude))
    c.execute(c.prepare(query_location), (row.userid, long(row.time),long(datetime.now().strftime("%H%M%S%f")), decimal.Decimal(row.longitude), decimal.Decimal(row.latitude)))

    query_remove = "INSERT INTO user_target (user_id,target_id,timestamp_produced, timestamp_spark,transaction_type) VALUES (?,?,?,?,?);"

    #Now, calculate distances between the user and his targets
    for target in r.smembers(row.userid):
        target_position = r.geopos(config.REDIS_LOCATION_NAME,target)[0] #geopos returns a list of tuples: [(longitude,latitude)], so to get the tuple out of the list, use [0]
        print('target id:',target, 'position:',target_position)
        print('type of variable: ',type(target_position))
        print('longitude:',target_position[0],'latitude:',target_position[1])
        print('distance between user:',row.userid,'and target:',target,'....')
        target_distance = get_distance(lon_1=decimal.Decimal(row.longitude),lat_1=decimal.Decimal(row.latitude),lon_2=decimal.Decimal(target_position[0]),lat_2=decimal.Decimal(target_position[1]))
        print('....',str(target_distance))
        if target_position <=SCORE_DIST:
            #if the user is within scoring distance, first make that call into cassandra
            c.execute(c.prepare(query_remove), (row.userid, new_target, long(row.time),long(datetime.now().strftime("%H%M%S%f")), -1))
            #then, fetch a new target
            redis_populate_targets(r,c,row)

    c.shutdown()



def debug_empty_rdd():
    print('received empty rdd')

def debug_save_user_in_redis(iter):
    member_list_name = 'member_list'
    
    #r = redis.StrictRedis(host='localhost', port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
    r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
    for record in iter:
        timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
        print('saving user:',record[0], 'timestamp_produced: ', float(record[1]), timestamp_spark_s)
        r.sadd(member_list_name,record[0])





def populate_user_targets_with_redis(r,record):
    possible_target_set = get_candidate_targets_with_redis(r,record)
    #print('fetched a possible set of',len(possible_target_set),'target locations')
    
    while (r.scard(record[0]+'_targets') < NUM_LOC_PER_USER) and (len(possible_target_set)>0):
        new_target = possible_target_set.pop()
        #print('adding member:', new_target,'to user:',record[0]) #I need to fix this anyway
        r.sadd(record[0]+'_targets',new_target)

def populate_user_targets_with_redis_and_cassandra(r,c,record):
    possible_target_set = get_candidate_targets_with_redis(r,record)

    query_insert_user_target = c.prepare("INSERT INTO user_target (user_id,target_id,timestamp_produced,timestamp_spark,transaction_type) VALUES (?,?,?,?,?);")
    query_insert_user_target.consistency_level = ConsistencyLevel.ANY

    while (r.scard(record[0]+'_targets') < NUM_LOC_PER_USER) and (len(possible_target_set)>0):
        new_target = possible_target_set.pop()
        timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
        c.execute(query_insert_user_target,(record[0], new_target, float(record[1])*1000, timestamp_spark_s*1000, 1))
        r.sadd(record[0]+'_targets',new_target)

def process_partition_with_redis(iter):
    #r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
    r_local = redis.StrictRedis(host='REDIS_DNS', port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
    for record in iter:
        #first, populate targets for the user if needed
        if r_local.scard(record[0]+'_targets') < NUM_LOC_PER_USER: #EDITED THIS FOR TESTING!! Need to revert later
            populate_user_targets_with_redis(r_local,record)
        #second, add the user location to the location timeseries database
        timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
        #print('adding user:',record[0],'lon: ',record[2],'lat: ',record[3],'timestamp_prod: ',record[1],'timestamp_spark_s: ',str(timestamp_spark_s))
        #r.zadd(record[0]+'_lon',long(float(record[1])*1000),record[2])
        #r.zadd(record[0]+'_lat',long(float(record[1])*1000),record[3])
        r_local.zadd(record[0]+'_time',long(float(record[1])*1000),long(float(timestamp_spark_s)*1000)) #EDITED THIS FOR TESTING!! Need to revert later

        for target in r_local.smembers(record[0]+'_targets'): #EDITED THIS FOR TESTING!! Need to revert later
            target_position = r_local.geopos(config.REDIS_LOCATION_NAME,target)[0] #geopos returns a list of tuples: [(longitude,latitude)], so to get the tuple out of the list, use [0]
            target_distance = get_distance(lon_1=decimal.Decimal(record[2]),lat_1=decimal.Decimal(record[3]),lon_2=decimal.Decimal(target_position[0]),lat_2=decimal.Decimal(target_position[1]))
            if target_position <=SCORE_DIST:
                #POP target
                r_local.srem(record[0]+'_targets',target) #EDITED THIS FOR TESTING!! Need to revert later
                populate_user_targets_with_redis(r_local,record) #EDITED THIS FOR TESTING!! Need to revert later

def process_partition_with_redis_and_cassandra(iter):
    redis_driver = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)
    cassandra_cluster = Cluster(config.CASSANDRA_DNS,protocol_version=3)
    cassandra_session = cassandra_cluster.connect(config.CASSANDRA_NAMESPACE)

    query_insert_user_location = cassandra_session.prepare("INSERT INTO user_location (user_id,timestamp_produced,timestamp_spark,longitude,latitude) VALUES (?,?,?,?,?);")
    query_insert_user_location.consistency_level = ConsistencyLevel.ANY

    query_insert_user_target = cassandra_session.prepare("INSERT INTO user_target (user_id,target_id,timestamp_produced,timestamp_spark,transaction_type) VALUES (?,?,?,?,?);")
    query_insert_user_target.consistency_level = ConsistencyLevel.ANY

    for record in iter:
        if redis_driver.scard(record[0]+'_targets') < NUM_LOC_PER_USER:
            #populate_user_targets_with_redis_and_cassandra(redis_driver,cassandra_session,record)
            possible_target_set = get_candidate_targets_with_redis(redis_driver,record)
            while (redis_driver.scard(record[0]+'_targets') < NUM_LOC_PER_USER) and (len(possible_target_set)>0):
                new_target = possible_target_set.pop()
                timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
                cassandra_session.execute(query_insert_user_target,(record[0], new_target, float(record[1])*1000, timestamp_spark_s*1000, 1))
                redis_driver.sadd(record[0]+'_targets',new_target)

        #second, add the user location to the location timeseries database
        timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
        cassandra_session.execute(query_insert_user_location,(record[0], float(record[1])*1000, timestamp_spark_s*1000, decimal.Decimal(record[2]), decimal.Decimal(record[3])))
        #print('adding user:',record[0],'lon: ',record[2],'lat: ',record[3],'timestamp_prod: ',record[1],'timestamp_spark_s: ',str(timestamp_spark_s))

        for target in redis_driver.smembers(record[0]+'_targets'):
            target_position = redis_driver.geopos(config.REDIS_LOCATION_NAME,target)[0] #geopos returns a list of tuples: [(longitude,latitude)], so to get the tuple out of the list, use [0]
            target_distance = get_distance(lon_1=decimal.Decimal(record[2]),lat_1=decimal.Decimal(record[3]),lon_2=decimal.Decimal(target_position[0]),lat_2=decimal.Decimal(target_position[1]))
            if target_position <=SCORE_DIST:
                #POP target
                timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
                cassandra_session.execute(query_insert_user_target,(record[0], target, float(record[1])*1000, timestamp_spark_s*1000, -1))
                redis_driver.srem(record[0]+'_targets',target)
                populate_user_targets_with_redis_and_cassandra(redis_driver,record)

    cassandra_cluster.shutdown()



def write_user_timeseries_to_cassandra(iter): 

    #set_max_connections_per_host(host_distance.LOCAL,36) host_distance.LOCAL = 0
    #set_max_connections_per_host(host_distance.REMOTE,36) host_distance.REMOTE = 1
    cassandra_cluster = Cluster(config.CASSANDRA_DNS,protocol_version=3)

    #cassandra_session = Cluster(config.CASSANDRA_DNS).connect(config.CASSANDRA_NAMESPACE)
    cassandra_session = cassandra_cluster.connect(config.CASSANDRA_NAMESPACE)

    #cassandra_session.cluster.set_max_connections_per_host(0, 36).set_max_connections_per_host(1, 36)

    insert_query = cassandra_session.prepare("INSERT INTO user_location (user_id,timestamp_produced, timestamp_spark,longitude,latitude) VALUES (?,?,?,?,?);")
    
    insert_query.consistency_level = ConsistencyLevel.ANY

    for record in iter:
    	#cassandra_session.execute(insert_query,(record[0], long(record[1]),long(datetime.now().strftime("%H%M%S%f")), decimal.Decimal(record[2]), decimal.Decimal(record[3])))
        timestamp_spark_s = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
        cassandra_session.execute(insert_query,(record[0], float(record[1])*1000, timestamp_spark_s*1000, decimal.Decimal(record[2]), decimal.Decimal(record[3])))

    cassandra_cluster.shutdown()
    #cassandra_session.shutdown()

def main():    
	
    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")
    
    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, 1)

    
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS}) \
                            .map(lambda message: message[1].split(';'))

    
    
    #write to cassandra: used to be slow, set quorum to ANY
    #kafkaStream.foreachRDD(lambda rdd : rdd.foreachPartition(write_user_timeseries_to_cassandra))

    #writing to redis #tested this but it was too slow
    #kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_partition_with_redis))

    #writing to redis and cassandra
    #kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_partition_with_redis_and_cassandra))

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
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.saveAsTextFile('logs/'+str(float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f")))))

    # Tests speeds of setting up a redis partition per partition
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_empty_function_per_partition))

    # Tests speeds of setting up a redis partition per iter, iterating through the partition
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_empty_function_per_iter))

    # Tests speeds of setting up a redis partition per partition with a new connection every partition, benchmark against empty function
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_per_partition_StrictRedis))

    # Tests speeds of setting up a redis partition per partition with a connection pool, benchmark against empty function
    # kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(test_redis_connection_per_partition_ConnectionPool))

    # filters if the element 0 of the split message = 1 (if the just_logged_in boolean = 1)
    DStream_new_users = kafkaStream.filter(lambda message : int(message[4]))

    DStream_new_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_new_user))

    DStream_returning_users = kafkaStream.filter(lambda message : not int(message[4]))

    DStream_returning_users.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(process_returning_user_pipe))


    ssc.start()
    ssc.awaitTermination()
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

def process_new_user(iter):
# function: add users into redis
    redis_connection = redis_handler().connection

    for record in iter:
        possible_target_set = get_candidate_targets_with_redis(redis_connection,record)

        for i in range(NUM_LOC_PER_USER):
            redis_connection.sadd(record[0]+'_targets',possible_target_set.pop()) 

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
        redis_pipe.geopos(config.REDIS_LOCATION_NAME,set_copy.pop(),set_copy.pop(),set_copy.pop())

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
    for record in iter:
        index = 0
        for target_coordinates in target_positions[index]:
            target_distance = get_distance(lon_1=decimal.Decimal(record[2]),lat_1=decimal.Decimal(record[3]),lon_2=decimal.Decimal(target_coordinates[0]),lat_2=decimal.Decimal(target_coordinates[1]))
            print('lon:',target_coordinates[0],'lat:',target_coordinates[1],'dist',target_distance)
        index += 1
    return


if __name__ == '__main__':

    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")

    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, config.SPARK_MICROBATCH_DURATION)

    
    test_speeds(ssc)
    #main()








