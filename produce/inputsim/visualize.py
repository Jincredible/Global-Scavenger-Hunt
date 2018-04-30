#!/anaconda2/bin/python

#Objective is to help visualize some of the test data using matplotlib

#import statements
import os
import sys
import pandas
import matplotlib.pyplot as pyplot
import numpy

# Date and time
import time
from datetime import datetime, timedelta


def dataframe_from_csv(fn_csv):
	if os.path.exists(fn_csv):
		df = pandas.read_csv(fn_csv, sep=',', header=0)
	return df




if __name__ == "__main__":
	fn_csv_GPS = sys.argv[1]
	#print dataframe_from_csv(fn_csv_GPS).values #prints out all values as an array
	#print dataframe_from_csv(fn_csv_GPS).shape #gets row and column dimensions of dataframe
	#print dataframe_from_csv(fn_csv_GPS).columns.values # prints out the values of the column headers
	df_GPS = dataframe_from_csv(fn_csv_GPS)
	print "num longitude elements: ", df_GPS['Longitude'].size
	print "num latitude elements: ", df_GPS['Latitude'].size
	#pyplot.plot(df_GPS['Longitude'].values,df_GPS['Latitude'].values)
	#pyplot.show()
	#print "first element of Longitude Series: ", df_GPS['Longitude'].values[0]
	#print "size of element of longitude Series: ", sys.getsizeof(df_GPS['Longitude'].values[0]), " bytes"


