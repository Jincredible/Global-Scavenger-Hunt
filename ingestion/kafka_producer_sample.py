#!/anaconda2/bin/python

#Objectives:
#1. Sample kafka producer for every second. used for setting up spark streaming

#Inputs:
#1. address string (example: localhost:9092)
#2. topic string (example: test)
#3. sender string (example: 0)

#import statements
import random
import sys
#import six
from datetime import datetime
#from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import time

class Producer(object):

    def __init__(self, addr, intopic):
        self.producer = KafkaProducer(bootstrap_servers=addr)
        self.topic = intopic

    def produce_msgs(self, source_symbol):
        price_field = random.randint(800,1400)
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
            self.producer.send(self.topic, message_info)
            time.sleep(1)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    topic = str(args[2])
    partition_key = str(args[3])
    prod = Producer(ip_addr,topic)
    prod.produce_msgs(partition_key) 