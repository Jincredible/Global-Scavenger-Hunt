#!/anaconda2/bin/python

#Objectives:
#1. Read generated GPS path data from an output file generated by sim_walking.py
#2. Every 2 seconds, send a packet of user location data into the kafka topic of the instance running kafka
# The structure of the outgoing data looks like this:
# user_id ; timestamp ; longitude ; latitude ; just_logged_in?

import sys
import csv
import time
from datetime import datetime
#from kafka.client import KafkaClient #do I need this to work?
from kafka.producer import KafkaProducer
import pandas


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    topic = str(args[2])
    input_path = 'sim_results' #this is the directory of sim_results
    producer = KafkaProducer(bootstrap_servers=ip_addr)

    num_rows = 500
    num_files= 450 #number of files we're going to process

    df = pandas.DataFrame.from_csv('sim_results/path0000999.csv',header=0,sep='/',index_col=None).head(num_rows)
    df.columns = ['user0000999']
    #print(df)
    for index in range(num_files):
    	#print('index:',index)
    	filename = input_path + '/' + 'path' + str("%07d" % (index,)) + '.csv'
    	username = 'user' + str("%07d" % (index,))
    	df_i = pandas.DataFrame.from_csv(filename,header=0,sep='/',index_col=None)
    	df_i.columns = [username]
    	df = df.join(df_i)
    	#print(filename)
    	#print(username)

    print('rows:',df.shape[0])
    print('cols:',df.shape[1])
    #print(df.columns.values.tolist())
    

    filenames =df.columns.values.tolist()
    df =df.transpose()

    print('rows:',df.shape[0])
    print('cols:',df.shape[1])
    #print(df.columns.values.tolist())
    
    #print df

    str_fmt = "{};{};{};{};{}"

    #print df
    
	

    for column in df:
    	row_i = 0
    	for row in df[column]:
    		#print('row:',row_i)
    		#print('col:',column)
    		timestamp = datetime.now().strftime("%Y%m%d%H%M%S") #There used to be a space in here!
    		longitude = row.split(',')[0]
    		latitude = row.split(',')[1]
    		user_id = filenames[row_i]
    		message_to_send = str_fmt.format(user_id, timestamp, longitude, latitude, int(column==0))
    		#print message_to_send

    		producer.send(topic=topic,value=message_to_send,key=user_id.encode('utf-8'))
    		row_i +=1

	







    