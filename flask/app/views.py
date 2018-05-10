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
	user = { 'nickname': 'Steven' } # sample user
	mylist = [1,2,3,4]
	return render_template("index.html", title = 'Home', user = user, mylist = mylist)

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
    mymap = Map(
        identifier="view-side",
        lat=37.4419,
        lng=-122.1419,
        markers=[(37.4419, -122.1419)]
    )
    sndmap = Map(
        identifier="sndmap",
        lat=37.4419,
        lng=-122.1419,
        markers=[
          {
             'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
             'lat': 37.4419,
             'lng': -122.1419,
             'infobox': "<b>Hello World</b>"
          },
          {
             'icon': 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
             'lat': 37.4300,
             'lng': -122.1400,
             'infobox': "<b>Hello World from other place</b>"
          }
        ]
    )
    return render_template('map_example.html', mymap=mymap, sndmap=sndmap)



#can we keep this line of code or does views have to continuously run? Answer: No.
#cassandra_cluster.shutdown()


# EXAMPLE API IMPEMENTATION FROM WIKI ===================================
# wiki URL: https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/Flask
'''
@app.route('/api/<email>/<date>')
def get_email(email,date):
	querystatement_select_email = "SELECT * FROM email WHERE id=%s AND date=$s"
	#query_select_email = cassandra_session.prepare("SELECT * FROM frontend_email WHERE id=%s AND date=%s;")
	response = cassandra_session.execute(querystatement_select_email, parameters=[email,date])
	response_list=[]
	for val in response:
		response_list.append(val)
	jsonresponse=[{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
	return jsonify(emails=jsonresponse)
'''


