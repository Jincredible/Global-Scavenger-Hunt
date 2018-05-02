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
# The parameters
# streaming_config.KAFKA_TOPIC: name of kafka topic for upstream queue
# streaming_config.KAFKA_DNS: public DNS and port for Kafka messages
# streaming_config.REDIS_DNS: public DNS and port for Redis instance
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

# getSqlContextInstance From Spark Streaming Tutorial -----------------------------------------
# http://spark.apache.org/docs/1.3.0/streaming-programming-guide.html#dataframe-and-sql-operations
# Lazily instantiated global instance of SQLContext

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

# --------------------------------------------------------------------------------------------

'''
def process_rdd(rdd):
    #print "========= %s =========" % str(time)
    
	try:
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd.context)

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        df = sqlContext.createDataFrame(rowRdd)

        # Register as table
        #df.registerTempTable("words")

        df.show()

    except:
        pass
   
    return rdd
'''

def main():

    user_id = StructField("user_id", StringType(), False)
    timestamp = StructField("timestamp", TimestampType(), False)
    longitude = StructField("longitude", DoubleType(), False)
    latitude = StructField("latitude", DoubleType(), False)
    just_logged_in = StructField("just_logged_in", BooleanType(), False)

    user_schema = StructType(user_id, timestamp, longitude, latitude, just_logged_in)
	# first, get the spark handler
    sc = SparkContext(appName="PysparkStreamingApp")
    sc.setLogLevel("WARN")
    
    # set microbatch interval as X seconds
    ssc = StreamingContext(sc, 3)

    #would have to set up a checkpoint directory, a check point folder for window process to run this command
    #ssc.checkpoint(config.CHECKPOINT_DIR) 
    
    # create a direct stream from kafka without using receiver
    kafkaStream = KafkaUtils.createDirectStream(ssc, [config.KAFKA_TOPIC], {"metadata.broker.list": config.KAFKA_DNS})
    #kafkaRDD=kafkaStream.map(lambda r: get_tuple(r))

    #kafkaStream.foreachRDD(process_rdd)

    #df = kafkaStream.map(lambda line: split_line(line[1]))
    # parse each record string as ; delimited
    #data_ds = kafkaStream.map(lambda v: v[1].split(config.MESSAGE_DELIMITER)) #reference code, slightly edited
    #kafkaStream.map(lambda v: process_each(v))
    #data_ds.count().map(lambda x:'Records in this batch: %s' % x)\
    #               .union(data_ds).pprint()
    kafkaStream.pprint()
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