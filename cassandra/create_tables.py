############################################################
# This script sets up the cassandra tables before each run 
# The parameters in cassandra_config
# cassandra_config.CASSANDRA_DNS: public DNS of cassandra seed 
# cassandra_config.CASSANDRA_NAMESPACE: namespace of database for tables
# were written in a separate "cassandra_config.py".
############################################################

from cassandra.cluster import Cluster
import cassandra_config as config

cluster = Cluster(config.CASSANDRA_DNS)
session = cluster.connect(config.CASSANDRA_NAMESPACE)


session.execute('DROP TABLE IF EXISTS user_location;')
session.execute('CREATE TABLE user_location (user_id text, time_stamp float, longitude float, latitude float, PRIMARY KEY (user_id, time_stamp)) WITH CLUSTERING ORDER BY (time_stamp ASC);')

session.execute('DROP TABLE IF EXISTS user_target;')
session.execute('CREATE TABLE user_target (user_id text, target_id text, time_stamp float, transaction_type int, PRIMARY KEY (user_id, target_id, time_stamp)) WITH CLUSTERING ORDER BY (time_stamp ASC);')
