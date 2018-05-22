#!/anaconda2/bin/python

#Objectives:
#1. input POI data into redis database

#import statements
import os
import sys
import random
import redis
import csv
import numpy
import pandas
from datetime import datetime
#global_config has the following variables:
#REDIS_PORT
#REDIS_DNS
#REDIS_PASS
#REDIS_RESET
#REDIS_DATABASE
import global_config as config

def dataframe_from_csv(fn_csv):
	if os.path.exists(fn_csv):
		df = pandas.read_csv(fn_csv, sep=',', header=0)
	return df[[0,1]]

def add_to_redis(r,df_in,str_in):

	for index, row in df_in.iterrows():
		#print("row[0]: ", str(row[0]), "row[1]: ", str(row[1]), "index: ", str(index))
		r.geoadd('Boston',row[0],row[1],str_in+str(index))

	return

def test_read(r,df_in,str_in):
	for index, row in df_in.iterrows():
		#print("row[0]: ", str(row[0]), "row[1]: ", str(row[1]), "index: ", str(index))
		position = r.geopos('Boston',str_in+str(index))
	return

def test_georadius(r,df_in):
	for index, row in df_in.iterrows():
		#print("row[0]: ", str(row[0]), "row[1]: ", str(row[1]), "index: ", str(index))
		target_set = r.georadius('Boston',row[0],row[1],100)
	return

def test1(fn_csv_Boston,fn_csv_Cambridge):
	r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)

	if config.REDIS_RESET:
		print('flushing database:', config.REDIS_DATABASE)
		r.flushdb()
	
	df_boston = dataframe_from_csv(fn_csv_Boston)
	df_cam = dataframe_from_csv(fn_csv_Cambridge)

	print 'test adding to sorted set'
	num_loops=10

	time_start = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	for i in range(num_loops):
		add_to_redis(r,df_boston,'bos')
		add_to_redis(r,df_cam,'cam')
	time_end = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))

	duration = time_end - time_start
	num_records = (df_boston.shape[0] + df_cam.shape[0]) * num_loops
	print 'time_start: ' + str(time_start) + ' time_end: ' + str(time_end)
	print 'duration: ' + str(duration) + ' records: ' + str(num_records) + ' records/s ' + str(float(num_records)/duration)
	
	num_loops=10
	print 'test reading geoposition from sorted set'
	time_start = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	for i in range(num_loops):
		test_read(r,df_boston,'bos')
		test_read(r,df_boston,'cam')
	time_end = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))

	duration = time_end - time_start
	num_records = (df_boston.shape[0] + df_cam.shape[0]) * num_loops
	print 'time_start: ' + str(time_start) + ' time_end: ' + str(time_end)
	print 'duration: ' + str(duration) + ' records: ' + str(num_records) + ' records/s ' + str(float(num_records)/duration)

	print 'fetching targets from sorted set using georadius'
	time_start = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	for i in range(num_loops):
		test_georadius(r,df_boston)
		test_georadius(r,df_boston)
	time_end = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	duration = time_end - time_start
	num_records = (df_boston.shape[0] + df_cam.shape[0]) * num_loops
	print 'time_start: ' + str(time_start) + ' time_end: ' + str(time_end)
	print 'duration: ' + str(duration) + ' records: ' + str(num_records) + ' records/s ' + str(float(num_records)/duration)
	#df_POI = dataframe_from_csv(fn_csv_Boston).append(dataframe_from_csv(fn_csv_Cambridge),ignore_index=True)
	#print df_POI_01
	#print df_POI_02
	#add_to_redis(df_POI)

def test2():
	fn_csv_Boston = 'POI_01.csv'
	fn_csv_Cambridge = 'POI_02.csv'

	df_boston = dataframe_from_csv(fn_csv_Boston)
	df_cam = dataframe_from_csv(fn_csv_Cambridge)

	r = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)

	print 'fetching targets from sorted set using georadius: no piping'
	time_start = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	for i in range(num_loops):
		test_georadius(r,df_boston)
		test_georadius(r,df_boston)
	time_end = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	duration = time_end - time_start
	num_records = (df_boston.shape[0] + df_cam.shape[0]) * num_loops
	print 'time_start: ' + str(time_start) + ' time_end: ' + str(time_end)
	print 'duration: ' + str(duration) + ' records: ' + str(num_records) + ' records/s ' + str(float(num_records)/duration)

	print 'fetching targets from sorted set using georadius: with piping'
	time_start = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	for i in range(num_loops):
		test_georadius(r,df_boston)
		test_georadius(r,df_boston)
	time_end = float(datetime.now().strftime("%M"))*60+float(datetime.now().strftime("%S.%f"))
	duration = time_end - time_start
	num_records = (df_boston.shape[0] + df_cam.shape[0]) * num_loops
	print 'time_start: ' + str(time_start) + ' time_end: ' + str(time_end)
	print 'duration: ' + str(duration) + ' records: ' + str(num_records) + ' records/s ' + str(float(num_records)/duration)

if __name__ == "__main__":
	
	test2()
	