#!/anaconda2/bin/python

#Objectives:
#1. Read generated GPS path data from an output file generated by sim_walking.py
#2. Every 2 seconds, send a packet of user location data into the kafka topic of the instance running kafka
# The structure of the outgoing data looks like this:
# user_id ; timestamp ; longitude ; latitude ; just_logged_in?

import sys
import csv
#import six #need to figure out what this does
import time
from datetime import datetime
from kafka.client import KafkaClient #do I need this to work?
from kafka.producer import KafkaProducer

class Producer(object):

    def __init__(self, addr, intopic, infile):
        self._producer = KafkaProducer(bootstrap_servers=addr)
        self._gpsfilename = infile
        self._topic = intopic

    def start_sending(self, user_id):
        
    	with open(self._gpsfilename) as infile:
    		just_logged_in = True
    		reader = csv.reader(infile)
    		for row in reader:
    			longitude = float(row[0])
    			latitude = float(row[1])
    			timestamp = datetime.now().strftime("%Y%m%d %H%M%S")
    			str_fmt = "{};{};{};{};{}"
    			message_to_send = str_fmt.format(user_id, timestamp, longitude, latitude, int(just_logged_in))
    			self.send_message(message_to_send,user_id)
    			just_logged_in = False
    			time.sleep(2) #sleep for 2 seconds
            
    def send_message(self,out_message,user_id):
    	print out_message
        #Kafka producer send documentation: send(topic, value=None, key=None, partition=None, timestamp_ms=None)[source]
    	self._producer.send(topic=self._topic,value=out_message,key=unicode(user_id))


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    topic = str(args[2])
    input_path_file = str(args[3])
    prod = Producer(ip_addr,topic, input_path_file)
    print input_path_file
    user_id = 'user0000007'
    prod.start_sending(user_id) 