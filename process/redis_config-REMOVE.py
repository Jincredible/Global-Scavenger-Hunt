# Streaming config file, read into streaming.py

#Redis variables
#REDIS_DNS='ec2-18-208-17-22.compute-1.amazonaws.com' #public host dns. DO NOT INCLUDE PORT
REDIS_PORT='6379' #Redis host port
REDIS_PASS='redispasswordinsight'
REDIS_RESET=1 #If 1, will reset database when calling import_POI_to_redis.py, calling r.flushdb()