############################################################
# This python script is the main script for spark streaming. 
# Here is the JSON format of the data from kafka:
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
from pyspark.sql import Row
from pyspark.sql.types import *

# Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json, math, datetime

# redis
import redis

# custom python configuration file we made to store the kafka parameters
import streaming_config as config


# Global variables
REDIS_DATABASE = 7 #7 is for testing, 0 is for production
NUM_LOC_PER_USER = 1 #This is the number of target locations for each user at each time
OUTER_RADIUS = 1000 #in meters, this is the max distance'




# getSqlContextInstance From Spark Streaming Tutorial -----------------------------------------
# http://spark.apache.org/docs/1.3.0/streaming-programming-guide.html#dataframe-and-sql-operations
# Lazily instantiated global instance of SQLContext

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# --------------------------------------------------------------------------------------------

#def redis_get_targets(row,r):
    #outerset = 

def process_row_redis(row):
    #processes the row, writes to redis

    # 1. can't pass any additional parameters to the foreach commands,
    # 2. encountered difficulty passing the redis handler in lambda. This is because we can't really broadcast this handler
    r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=REDIS_DATABASE, password=config.REDIS_PASS) #OBVIOUSLY GOING TO BE A BOTTLENECK

    #first, check if the user_id exists in redis
    #command is r.exists(key_name)
    if r.exists(row.userid): #does the user exist in the database?
        print('user:',row.userid,'does exist in database')
        #if r.scard(row.userid) < NUM_LOC_PER_USER: #is the number of target locations less than the number of targets the user is supposed to have?

    else:
        print('user:',row.userid,'does NOT exist in database')
        #if this key doesn't exist in the database, we're going to have to"
        #   1. get N neighbors
        #   2. populate the Hash Set


def process_row(row):
    #row_string = 'userid: '+row[0]+' time: '+row[1]+' longitude: '+ row[2]+ ' latitude: '+ row[3]+ ' just_logged_in: '+ row[4]
    #SparkContext(appName="PysparkStreamingApp").parallelize(row)
    #re_row = Row(row_string=row_string)
    #re_row_rdd = SparkContext(appName="PysparkStreamingApp").parallelize(re_row)
    #re_row_rdd.take(5)
    #print('userid: '+row[0]+' time: '+row[1]+' longitude: '+ row[2]+ ' latitude: '+ row[3]+ ' just_logged_in: '+ row[4])
    print('using row.<column_name> convention:')
    print('userid: '+row.userid+' time: '+row.time+' longitude: '+ DoubleType(row.longitude)+ ' latitude: '+ DoubleType(row.latitude)+ ' just_logged_in: '+ BooleanType(row.just_logged_in))


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
        rowRdd = rdd.map(lambda x: Row(userid=x[0], time=x[1], longitude=x[2],latitude=x[3],just_logged_in=x[4]))
        df = spark.createDataFrame(rowRdd)
        df.show()
        #df.foreach(process_row)
        #can only pass one argument into the foreach command. try using lambda
        df.foreach(process_row_redis)



def main():    
	
    # first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")
    
    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, 3)

    #would have to set up a checkpoint directory, a check point folder for window process to run this command
    #ssc.checkpoint(config.CHECKPOINT_DIR) 
    
    # create a direct stream from kafka without using receiver. Each message is coming in as a tuple of pairs. Because no key was specified
    # in kafka, the kafka messages look like this (NONE, <message as unicode>). 
    '''
    -------------------------------------------
    Time: 2018-05-02 19:18:48
    -------------------------------------------
    (None, u'0007.csv;20180502 191845;-71.1436077241;42.3948464553;0')
    (None, u'0007.csv;20180502 191847;-71.1435865867;42.3948374514;0')
    '''
    #This is why we need to get the second half of the tuple pair by
    # mapping it as *.map(lambda x: x[1]).
    # As a result, the resulting stream looks like this:
    '''
    -------------------------------------------
    Time: 2018-05-02 19:27:18
    -------------------------------------------
    0007.csv;20180502 192715;-71.144326396;42.3951525891;0
    0007.csv;20180502 192717;-71.1443052585;42.3951435852;0
    '''
    # Then, we also add a .split(';') to each message so that the results look like this:
    '''
    -------------------------------------------
    Time: 2018-05-02 19:42:03
    -------------------------------------------
    [u'0007.csv', u'20180502 194200', u'-71.1441784341', u'42.3950895616', u'0']
    [u'0007.csv', u'20180502 194202', u'-71.1441572967', u'42.3950805576', u'0']
    '''
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS}) \
                            .map(lambda message: message[1].split(';'))
    
    #Now that we've applied a map to the Direct stream, this object is now a KafkaTransformedDStream object
    

    #When we create the foreachRDD command, we're going to remove the .pprint() command. What we're doing here is
    #Creating dataframes from the datastream, then printing the dataframes. This is what the results look like:
    '''
    +--------------+-------------+--------------+---------------+--------+
    |just_logged_in|     latitude|     longitude|           time|  userid|
    +--------------+-------------+--------------+---------------+--------+
    |             0|42.3950895616|-71.1441784341|20180502 200851|0007.csv|
    |             0|42.3950805576|-71.1441572967|20180502 200853|0007.csv|
    +--------------+-------------+--------------+---------------+--------+
    '''


    #we're going to use the .foreachRDD function to get the RDD of the datastream
    kafkaStream.foreachRDD(process_rdd)

    #df = kafkaStream.map(lambda line: split_line(line[1]))
    # parse each record string as ; delimited
    #data_ds = kafkaStream.map(lambda v: v[1].split(config.MESSAGE_DELIMITER)) #reference code, slightly edited
    #kafkaStream.map(lambda v: process_each(v))
    #data_ds.count().map(lambda x:'Records in this batch: %s' % x)\
    #               .union(data_ds).pprint()
    #kafkaStream.pprint()
    #df.pprint()

    ''' Commented reference code
    # use the window function to group the data by window
    dataWindow_ds = data_ds.map(lambda x: (x['userid'], (x['acc'], x['time']))).window(10,10)
    
    '''
    ''' This section was previously commented as well
    calculate the window-avg and window-std
    1st map: get the tuple (key, (val, val*val, 1)) for each record
    reduceByKey: for each key (user ID), sum up (val, val*val, 1) by column
    2nd map: for each key (user ID), calculate window-avg and window-std, return (key, (avg, std)) 
    '''
    ''' Commented reference code
    dataWindowAvgStd_ds = dataWindow_ds\
           .map(getSquared)\
           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
           .map(getAvgStd)
    
    # join the original Dstream with individual record and the aggregated Dstream with window-avg and window-std 
    joined_ds = dataWindow_ds.join(dataWindowAvgStd_ds)

    # label each record 'safe' or 'danger' by comparing the data with the window-avg and window-std    
    result_ds = joined_ds.map(labelAnomaly)
    resultSimple_ds = result_ds.map(lambda x: (x[0], x[1], x[5]))

    # Send the status table to rethinkDB and all data to cassandra    
    result_ds.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))
    resultSimple_ds.foreachRDD(sendRethink)
    '''

    ssc.start()
    ssc.awaitTermination()
    return

if __name__ == '__main__':
    main()