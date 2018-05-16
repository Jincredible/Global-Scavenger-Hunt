#!/anaconda2/bin/python

#Objective is to help visualize some of the test data using matplotlib

#import statements
import os
import sys
import pandas
import matplotlib.pyplot as pyplot
import numpy


def dataframe_from_csv(fn_csv):
	if os.path.exists(fn_csv):
		df = pandas.read_csv(fn_csv, sep=',', header=0)
	return df




if __name__ == "__main__":
	fn_csv_Hydrant = sys.argv[1]
	fn_csv_Walking = sys.argv[2]
	#print dataframe_from_csv(fn_csv_GPS).values #prints out all values as an array
	#print dataframe_from_csv(fn_csv_GPS).shape #gets row and column dimensions of dataframe
	#print dataframe_from_csv(fn_csv_GPS).columns.values # prints out the values of the column headers
	df_Hydrant = dataframe_from_csv(fn_csv_Hydrant)
	df_Walking = dataframe_from_csv(fn_csv_Walking)
	#pyplot.hold(True) #deprecated?
	pyplot.scatter(df_Hydrant['X'].values,df_Hydrant['Y'].values,s=1,marker=".")
	pyplot.plot(df_Walking['Longitude'].values,df_Walking['Latitude'].values,c='r')
	pyplot.show()
	
