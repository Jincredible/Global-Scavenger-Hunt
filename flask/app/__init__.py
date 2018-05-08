#!/usr/bin/env python

#Objectives:
#1. Create application object, imports view module

from flask import Flask
app = Flask(__name__)
from app import views

