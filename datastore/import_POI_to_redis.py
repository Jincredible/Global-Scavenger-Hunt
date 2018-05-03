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
#datastore_config has the following variables:
#REDIS_PORT
#REDIS_DNS
#REDIS_PASS
import datastore_config as config


def dataframe_from_csv(fn_csv):
	if os.path.exists(fn_csv):
		df = pandas.read_csv(fn_csv, sep=',', header=0)
	return df[[0,1]]

def add_to_redis(df_in,str_in,database_in):
	r = redis.StrictRedis(host='localhost', port=config.REDIS_PORT, db=database_in, password=config.REDIS_PASS)

	for index, row in df_in.iterrows():
		#print("row[0]: ", str(row[0]), "row[1]: ", str(row[1]), "index: ", str(index))
		r.geoadd('Boston',row[0],row[1],str_in+str(index))

	return



if __name__ == "__main__":
	fn_csv_Boston = sys.argv[1]
	fn_csv_Cambridge = sys.argv[2]
	database_num = sys.argv[3]
	add_to_redis(dataframe_from_csv(fn_csv_Boston),'bos',database_num)
	add_to_redis(dataframe_from_csv(fn_csv_Cambridge),'cam',database_num)
	#df_POI = dataframe_from_csv(fn_csv_Boston).append(dataframe_from_csv(fn_csv_Cambridge),ignore_index=True)
	#print df_POI_01
	#print df_POI_02
	#add_to_redis(df_POI)