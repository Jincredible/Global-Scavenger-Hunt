#!/usr/bin/env python

#Objectives:
#1. Handler that responds from web browsers

from app import app

@app.route('/')
@app.route('/index')
def index():
	return "Hello, World"


