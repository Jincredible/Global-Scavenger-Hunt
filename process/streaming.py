############################################################
# This python script is the main script for spark streaming. 
# Here is the JSON format of the data from kafka:
#
# {"userid": text, 
#  "time": timestamp, 
#  "acc": float}
# The "acc" column is the acceleration of the user.
#
# The main tasks of thise script is the following:
#
# 1. Receive streaming data from kafka as a Dstream object 
# 2. Take the original Dstream, calculate the window-average,
#    and window-standard-deviation for each user and window,
#    and produce a aggregated Dstream.
# 3. Join the original Dstream with the aggregated Dstream 
#    as a new Dstream
# 4. Using the window-avg and window-std from the aggregated Dstream,
#    label each record from the original Dstream as 'safe' or 'danger'
# 5. Send the list of (userid, status) to rethinkDB
# 6. Send all of the data to cassandra
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of kafka servers
# config.CHECKPOINT_DIR: check point folder for window process  
# config.ANOMALY_CRITERIA: if abs(data - avg) > config.ANOMALY_CRITERIA * std,
#                          then data is an anomaly
# config.RETHINKDB_SERVER: public DNS of the rethinkDB server
# config.RETHINKDB_DB: name of the database in rethinkDB
# config.RETHINKDB_TABLE: name of the table in rethinkDB
# config.CASSANDRA_SERVERS: public DNS and port of cassandra servers
# config.CASSANDRA_NAMESPACE: namespace for cassandra

# were written in a separate "config.py".
############################################################