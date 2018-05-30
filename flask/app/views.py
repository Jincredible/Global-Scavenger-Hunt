#!/usr/bin/env python

#Objectives:
#1. Handler that responds from web browsers

# jsonify creates a json representation of the response
from flask import jsonify

from flask import render_template, request

from flask import Flask

from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map

from app import app

import global_config as config
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# redis
import redis

# Setting up connections to cassandra
cassandra_cluster = Cluster(config.CASSANDRA_DNS)
cassandra_session = cassandra_cluster.connect(config.CASSANDRA_NAMESPACE)

redis_driver = redis.StrictRedis(host=config.REDIS_DNS, port=config.REDIS_PORT, db=config.REDIS_DATABASE, password=config.REDIS_PASS)

#initialize GoogleMaps extension
GoogleMaps(app, key=config.GOOGLE_MAPS_API_KEY)


@app.route('/index')
def index():
	return render_template("data_input.html")

@app.route('/index', methods=['POST'])
def index_post():
	user_id = request.form["user_id"]
	if not user_id:
		user_id="user0000000"
	querystatement_select_user_location = "SELECT * FROM user_location WHERE user_id=%s ORDER BY timestamp_produced DESC LIMIT 100"
	cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[user_id])
	response_list = []
	for val in cassandra_response:
		response_list.append(val)

	user_location_output = [{"user_id": x.user_id, 
				  			 "timestamp_produced": x.timestamp_produced, 
				  			 "latitude": x.latitude, 
				  			 "longitude": x.longitude, 
				  			 "timestamp_spark": x.timestamp_spark} for x in response_list]

	return render_template("data_output.html", user_location_output=user_location_output)


@app.route('/base')
def base():
	return render_template("user_location_in.html")

# Cassandra user_location table column names:
# user_id | timestamp_produced | latitude | longitude | timestamp_spark

@app.route("/base", methods=['POST'])
def base_post():
	user_id = request.form["user_id"]
	querystatement_select_user_location = "SELECT * FROM user_location WHERE user_id=%s;"
	cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[user_id])
	response_list = []
	for val in cassandra_response:
		response_list.append(val)
	jsonresponse = [{"user_id": x.user_id, "timestamp_produced": x.timestamp_produced, "latitude": x.latitude, "longitude": x.longitude, "timestamp_spark": x.timestamp_spark} for x in response_list]
	return render_template("user_location_out.html", output=jsonresponse)

@app.route('/realtime_example')
def realtime_example():
	return render_template("realtime_example.html")

@app.route('/slides')
def slides():
	return render_template("slides.html")

@app.route('/')
@app.route('/map_example')
def mapview():
    
    querystatement_select_user_location = "SELECT * FROM user_location WHERE user_id=%s ORDER BY timestamp_produced DESC LIMIT 100"
    
    num_users_to_simulate = 100
    user_location_output=[]
    user_id_output=[]

    for i in range(num_users_to_simulate):
    	user_location_output.append([])
    	username = 'user' + str("%07d" % (i,))
    	user_id_output.append(username)
    	cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[username])
    	for val in cassandra_response:
			#this json structure exists specifically because it's what google maps api accepts for their polyline obj
			user_location_output[i].append({"lat": val.latitude, 
					  			 			"lng": val.longitude})
    return render_template('map_example.html',user_location_output=user_location_output,user_id_output=user_id_output)

@app.route('/user_example')
def map_user():
	username = 'user0000071'

	querystatement_select_user_location = "SELECT * FROM user_location WHERE user_id=%s ORDER BY timestamp_produced DESC LIMIT 300"

	num_targets = redis_driver.scard(username+'_targets')
	target_names = redis_driver.smembers(username+'_targets')

	user_location_output=[]
	target_location_output=[]
	target_name_output=[]

	try:
		cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[username])
	except:
		username = 'user0000001'
		cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[username])

	for val in cassandra_response:
		#this json structure exists specifically because it's what google maps api accepts for their polyline obj
		user_location_output.append({"lat": val.latitude, 
				  			 		 "lng": val.longitude})

	for name in target_names:
		target_name_output.append(name)
		target_position = redis_driver.geopos(config.REDIS_LOCATION_NAME,name)[0]
		target_location_output.append({"lat": target_position[1], 
				  			 		   "lng": target_position[0]})

	return render_template('map.html',user_location_output=user_location_output,user_id_output=username,target_location_output=target_location_output,target_name_output=target_name_output)


@app.route('/map')
def map_user():
	username = request.args.get('username')

	querystatement_select_user_location = "SELECT * FROM user_location WHERE user_id=%s ORDER BY timestamp_produced DESC LIMIT 300"

	num_targets = redis_driver.scard(username+'_targets')
	target_names = redis_driver.smembers(username+'_targets')

	user_location_output=[]
	target_location_output=[]
	target_name_output=[]

	try:
		cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[username])
	except:
		username = 'user0000001'
		cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=[username])

	for val in cassandra_response:
		#this json structure exists specifically because it's what google maps api accepts for their polyline obj
		user_location_output.append({"lat": val.latitude, 
				  			 		 "lng": val.longitude})

	for name in target_names:
		target_name_output.append(name)
		target_position = redis_driver.geopos(config.REDIS_LOCATION_NAME,name)[0]
		target_location_output.append({"lat": target_position[1], 
				  			 		   "lng": target_position[0]})

	return render_template('map.html',user_location_output=user_location_output,user_id_output=username,target_location_output=target_location_output,target_name_output=target_name_output)
#can we keep this line of code or does views have to continuously run? Answer: No.
#cassandra_cluster.shutdown()

