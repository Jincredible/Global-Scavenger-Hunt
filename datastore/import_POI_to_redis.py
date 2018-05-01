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

def dataframe_from_csv(fn_csv):
	if os.path.exists(fn_csv):
		df = pandas.read_csv(fn_csv, sep=',', header=0)
	return df[[0,1]]

def add_to_redis(df_in):
	r = redis.StrictRedis(host='localhost', port=6379, db=0)

	for index, row in df.iterrows():
		r.geoadd('Boston',row['X'],row['Y'],index+10000)

	return



if __name__ == "__main__":
	fn_csv_Boston = sys.argv[1]
	fn_csv_Cambridge = sys.argv[2]
	df_POI = dataframe_from_csv(fn_csv_Boston).append(dataframe_from_csv(fn_csv_Cambridge),ignore_index=True)
	add_to_redis(df_POI)