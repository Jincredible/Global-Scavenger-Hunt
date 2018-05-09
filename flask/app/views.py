#!/usr/bin/env python

#Objectives:
#1. Handler that responds from web browsers

# jsonify creates a json representation of the response
from flask import jsonify

from app import app


# importing Cassandra modules from the driver we just installed
#from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
#cluster = Cluster(['<seed-node-public-dns>'])



@app.route('/')
@app.route('/index')
def index():
	return "Hello, World"


