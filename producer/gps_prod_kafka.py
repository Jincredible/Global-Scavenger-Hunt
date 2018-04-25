#!/anaconda2/bin/python

#Objectives:
#1. Read generated GPS path data from an output file generated by sim_walking.py
#2. Every 2 seconds, send a packet of user location data into the kafka topic of the instance running kafka
# The structure of the outgoing data looks like this:
# user_id ; longitude ; latitude ; timestamp ; just_logged_in?

import sys
import csv
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer

class Producer(object):

    def __init__(self, addr, infile):
        self.producer = KafkaProducer(bootstrap_servers=addr)
        self.gpsfile = open(infile, 'r')

    def produce_msgs(self, source_symbol):
        

        while True:
            time_field = datetime.now().strftime("%Y%m%d %H%M%S")
            price_field += random.randint(-10, 10)/10.0
            volume_field = random.randint(1, 1000)
            str_fmt = "{};{};{};{}"
            message_info = str_fmt.format(source_symbol,
                                          time_field,
                                          price_field,
                                          volume_field)
            print message_info
            self.producer.send('test', message_info)
            



if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    input_path_file = str(args[3])
    prod = Producer(ip_addr,infile)
    prod.produce_msgs(partition_key) 