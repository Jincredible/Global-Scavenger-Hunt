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


# Setting up connections to cassandra
cassandra_cluster = Cluster(config.CASSANDRA_DNS)
cassandra_session = cassandra_cluster.connect(config.CASSANDRA_NAMESPACE)

#initialize GoogleMaps extension
GoogleMaps(app, key=config.GOOGLE_MAPS_API_KEY)

@app.route('/')
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

@app.route('/map_example')
def mapview():
    # creating a map in the view
    querystatement_select_user_location = "SELECT * FROM user_location WHERE user_id=%s ORDER BY timestamp_produced DESC LIMIT 100"
    cassandra_response = cassandra_session.execute(querystatement_select_user_location, parameters=['user0000007'])
    response_list = []

    for val in cassandra_response:
    	#print('val user_id:', val.user_id, 'timestamp:', val.timestamp_produced)
        response_list.append(val)

    user_location_output = [{"user_id": x.user_id, 
				  			 "timestamp_produced": x.timestamp_produced, 
				  			 "latitude": x.latitude, 
				  			 "longitude": x.longitude, 
				  			 "timestamp_spark": x.timestamp_spark} for x in response_list]
    return render_template('map_example.html',user_location_output=user_location_output)



#can we keep this line of code or does views have to continuously run? Answer: No.
#cassandra_cluster.shutdown()

