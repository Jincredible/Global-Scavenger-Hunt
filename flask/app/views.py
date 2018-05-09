#!/usr/bin/env python

#Objectives:
#1. Handler that responds from web browsers

# jsonify creates a json representation of the response
from flask import jsonify

from flask import render_template

from app import app

import global_config as config
# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster


# Setting up connections to cassandra
cassandra_cluster = Cluster(config.CASSANDRA_DNS)
cassandra_session = cluster.connect(config.CASSANDRA_NAMESPACE)

@app.route('/')
@app.route('/index')
def index():
	user = { 'nickname': 'Miguel' } # fake user
	return render_template("index.html", title = 'Home', user = user)


#can we keep this line of code or does views have to continuously run?
cassandra_cluster.shutdown()


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


