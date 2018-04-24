#!/anaconda2/bin/python

#Objectives:
#1. Generate random walking with the redis GEO database

#import statements
import os
import sys
import random
import redis
import csv
from itertools import izip
import numpy

MIN_MEMBER_NUM=10000 #Defined in the commands to redis txt file, generated by the get_POI.py script
MAX_MEMBER_NUM=25738 #Defined in the commands to redis txt file, dependent on the number of values in the CSV file passed into the get_POI.py script

KEY_VALUE = "Boston"
MIN_RADIUS = 100 #in meters, this is the first radius we use to search
MAX_RADIUS = 450 #in meters, this is the max radius we are willing to search
STEP_RADIUS = 75 #in meters, if we don't find a valid set, step up the radius with this
NUM_PAST_MEMBERS = 4 #minimum is one, which is where you currently are

WALKING_DURATION = 50 #in number of members after starting point, this is the max number of members we will simulate each player walking to
NUM_SAMPLES = 1000

WALKING_SPEED = 1 #in meters per second, walking speed of user
TIME_BETWEEN_UPDATES = 2 #in seconds, time between the GPS locations are updated

RESULTS_PATH = 'sim_results'


def find_neighbor(redis_handle, recent_members, radius=MIN_RADIUS):
	neighbors = redis_handle.georadiusbymember(KEY_VALUE,recent_members[-1],radius,unit="m")
	subset = set(neighbors) - set(recent_members)
	if len(subset) > 0:
		return random.sample(subset,1)
	else:
		if radius >= MAX_RADIUS:
			print("ERROR, radius has reached MAX_RADIUS, returning", recent_members[0])
			return recent_members[0]
		else:
			return find_neighbor(redis_handle,recent_members,radius+STEP_RADIUS)

def interpolate_GPS(start_GPS,dest_GPS,num_bins): #because we are assuming that the distance between these two points are relatively small (under 300m, we will assume a flat plane)
	#print ('start_GPS:', start_GPS)
	#print ('dest_GPS:', dest_GPS)
	#print ('num_bins:', num_bins)
	if num_bins== 0:
		return start_GPS
	else: 
		x_step = (dest_GPS[0]-start_GPS[0])/num_bins
		y_step = (dest_GPS[1]-start_GPS[1])/num_bins
	
	if x_step == 0:
		x_range = numpy.ones(num_bins-1)*start_GPS[0]
	else:
		x_range = numpy.arange(start_GPS[0],dest_GPS[0],x_step)[0:num_bins-1]

	if y_step == 0:
		y_range = numpy.ones(num_bins-1)*start_GPS[1]
	else:
		y_range = numpy.arange(start_GPS[1],dest_GPS[1],y_step)[0:num_bins-1]

	path_GPS_array = numpy.vstack((x_range,y_range)).T
	return path_GPS_array.tolist() #this is the interpolated path between the start and destination, a list of tuples

def write_clean_file(filename,values): #pass filename without extension!!
	
	try:
		with open(RESULTS_PATH+'/'+filename+'_temp.csv', 'w') as f:
			writer = csv.writer(f)
			writer.writerows(values)

		with open(RESULTS_PATH+'/'+filename+'_temp.csv', 'r') as infile, open(RESULTS_PATH+'/'+filename+'.csv', 'w') as outfile:
			data = infile.read()
			data = data.replace('"', '')
			data = data.replace('(', '')
			data = data.replace(')', '')
			outfile.write(data)
			outfile.close()

		os.remove(RESULTS_PATH+'/'+filename+'_temp.csv')
	except csv.Error: #I need to figure out what causes this error!!
		print 'csv error has occured for', filename
		#print 'values:'
		#print values
		pass

def write_file(filename,values):
	try:
		with open(RESULTS_PATH+'/'+filename+'.csv', 'w') as f:
			writer = csv.writer(f)
			writer.writerows(values)

	except csv.Error: #I need to figure out what causes this error!!
		print 'csv error has occured for', filename
		#print 'values:'
		#print values
		pass

def generate_walk(tag='0000000'):
	r = redis.StrictRedis(host='localhost', port=6379, db=0) #first, define the python redis handler
	
	members = []
	locations = [] #list of tuples
	user_GPS = [] #list of lists

	#The first member is randomly generated
	members.append(str(random.randint(MIN_MEMBER_NUM,MAX_MEMBER_NUM)))

	#The locations list will need to be udpated with the member's coordinates (in this case, it will always be the last member in list)
	#We're using the extend function instead of the append function because geopos function in redis returns a list of tuples
	locations.extend(r.geopos(KEY_VALUE,members[-1]))
	

	for count in range(WALKING_DURATION):
		#there are some times when the simulation walks into a corner and can't get out
		try:
			members.extend(find_neighbor(r,members[-NUM_PAST_MEMBERS:],MIN_RADIUS))
			locations.extend(r.geopos(KEY_VALUE,members[-1]))
			
		except redis.exceptions.ResponseError: #in those cases when the simulation goes into a corner, redis throws a response error
			pass
		try:
			bins = int(r.geodist(KEY_VALUE,members[-2],members[-1])/(WALKING_SPEED*TIME_BETWEEN_UPDATES))
		except TypeError:
			print 'num of members:', len(members)
			print 'members: ', members
			print 'current member:', members[-1]
			print 'previous member: ', members[-2]
		user_GPS.extend(interpolate_GPS(start_GPS=locations[-1],dest_GPS=locations[-2],num_bins=bins)) #need to update!

	if len(members) > 1: 
		write_clean_file(filename='members'+tag,values=izip(members,locations)) #Need to pass the filename, without extension!
		write_file(filename='path'+tag,values=user_GPS)

if __name__ == "__main__":

	for i in range(NUM_SAMPLES):
		generate_walk(str("%07d" % (i,)))

	


