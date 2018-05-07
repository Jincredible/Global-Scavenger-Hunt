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
import streaming_config as config


# Global variables
REDIS_DATABASE = 7 #7 is for testing, 0 is for production
NUM_LOC_PER_USER = 3 #This is the number of target locations for each user at each time
MIN_LOC_PER_USER = 1 # this is in case there aren't enough target locations within the MAX_OUTER_RADIUS
OUTER_RADIUS = 600 #in meters, this is the outer bound distance to fetch target location
MAX_OUTER_RADIUS = 2500 #in meters, this is the maximum distance to fetch targets
INNER_RADIUS = 400 #in meters, this is the inner bound distance to fetch target location
SCORE_DIST = 30 #in meters, distance a player must be to score the point
REDIS_LOCATION_NAME='Boston'



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


def redis_get_new_targets(r,row,out_radius=OUTER_RADIUS,in_radius=INNER_RADIUS): #returns a set of possible locations
    set_outer = set(r.georadius(name=REDIS_LOCATION_NAME, longitude=row.longitude, latitude=row.latitude, radius=out_radius, unit='m'))
    set_inner = set(r.georadius(name=REDIS_LOCATION_NAME, longitude=row.longitude, latitude=row.latitude, radius=in_radius, unit='m'))
    #also, implement add another set of 'SOLVED' targets for this particular user
    set_targets = set_outer - set_inner
    if (len(set_targets) < MIN_LOC_PER_USER) or (out_radius >= MAX_OUTER_RADIUS):
        return redis_get_new_targets(r,row,out_radius+100, in_radius)
    else:
        return set_targets

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


def process_row_redis(row):
    #processes the row, writes to redis

    # 1. can't pass any additional parameters to the foreach commands,
    # 2. encountered difficulty passing the redis handler in lambda. This is because we can't really broadcast this handler
    r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=REDIS_DATABASE, password=config.REDIS_PASS) #OBVIOUSLY GOING TO BE A BOTTLENECK
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
        target_position = r.geopos(REDIS_LOCATION_NAME,target)[0] #geopos returns a list of tuples: [(longitude,latitude)], so to get the tuple out of the list, use [0]
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


#def process_row(row):
    #row_string = 'userid: '+row[0]+' time: '+row[1]+' longitude: '+ row[2]+ ' latitude: '+ row[3]+ ' just_logged_in: '+ row[4]
    #SparkContext(appName="PysparkStreamingApp").parallelize(row)
    #re_row = Row(row_string=row_string)
    #re_row_rdd = SparkContext(appName="PysparkStreamingApp").parallelize(re_row)
    #re_row_rdd.take(5)
    #print('userid: '+row[0]+' time: '+row[1]+' longitude: '+ row[2]+ ' latitude: '+ row[3]+ ' just_logged_in: '+ row[4])
    #print('using row.<column_name> convention:')
    #print('userid: '+row.userid+' time: '+row.time+' longitude: '+ DoubleType(row.longitude)+ ' latitude: '+ DoubleType(row.latitude)+ ' just_logged_in: '+ BooleanType(row.just_logged_in))


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process_rdd(rdd):
    # this is the redis handler
    
    #first, need to check if RDD has any elements
    if rdd.isEmpty():
        return
    else:
        spark = getSparkSessionInstance(rdd.context.getConf())

        # convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda x: Row(userid=x[0], time=x[1].replace(" ",""), longitude=x[2],latitude=x[3],just_logged_in=x[4]))
        df = spark.createDataFrame(rowRdd)
        #df.show()
        
        df.foreach(process_row_redis)


def debug_empty_rdd():
    print('received empty rdd')

def debug_save_user_in_redis(iter):
    member_list_name = 'member_list'

    r = redis.StrictRedis(host='localhost', port=config.REDIS_PORT, db=REDIS_DATABASE, password=config.REDIS_PASS)
    for record in iter:
        r.sadd(member_list_name,record[0])


def write_user_timeseries_to_cassandra(iter): #This is too slow. need to find out how to speed up cassandra writes
	#cluster = Cluster(config.CASSANDRA_DNS)
    #in this case, c is session (session=cluster.connect(<namespace>))
    cassandra_session = Cluster(config.CASSANDRA_DNS).connect(config.CASSANDRA_NAMESPACE)

    insert_query = cassandra_session.prepare("INSERT INTO user_location (user_id,timestamp_produced, timestamp_spark,longitude,latitude) VALUES (?,?,?,?,?);")

    for record in iter:
    	cassandra_session.execute(insert_query,(record[0], long(record[1]),long(datetime.now().strftime("%H%M%S%f")), decimal.Decimal(record[2]), decimal.Decimal(record[3])))

    cassandra_session.shutdown()

def main():    
	
    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")
    
    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, 1)

    
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS}) \
                            .map(lambda message: message[1].split(';'))
    
    
    #write to cassandra: put on hold for now
    #kafkaStream.foreachRDD(lambda rdd : rdd.foreachPartition(write_user_timeseries_to_cassandra))

    kafkaStream.foreachRDD(lambda rdd : None if rdd.isEmpty() else rdd.foreachPartition(debug_save_user_in_redis))

    ssc.start()
    ssc.awaitTermination()
    return

if __name__ == '__main__':
    main()






