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
# streaming-config.KAFKA_TOPIC: name of kafka topic for upstream queue
# streaming-config.KAFKA_DNS: public DNS and port for Kafka messages
# streaming-config.REDIS_DNS: public DNS and port for Redis instance
# streaming-config.REDIS_PASS: password for redis authentication
# were written in a separate "streaming-config.py".
############################################################