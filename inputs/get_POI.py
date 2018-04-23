#!/anaconda2/bin/python

#Objective is to help visualize some of the test data using matplotlib

#import statements
import os
import sys
import pandas
import matplotlib.pyplot as pyplot
import numpy
import csv


def dataframe_from_csv(fn_csv):
	if os.path.exists(fn_csv):
		df = pandas.read_csv(fn_csv, sep=',', header=0)
	return df[['X','Y']]




if __name__ == "__main__":
	fn_csv_Boston = sys.argv[1]
	fn_csv_Cambridge = sys.argv[2]
	#df_Boston = dataframe_from_csv(fn_csv_Boston)
	#df_Cambridge = dataframe_from_csv(fn_csv_Cambridge)
	#pyplot.scatter(df_Boston['X'].values,df_Boston['Y'].values,s=1,marker=".")
	#pyplot.scatter(df_Cambridge['X'].values,df_Cambridge['Y'].values,s=1,marker=".")
	df_POI = dataframe_from_csv(fn_csv_Boston).append(dataframe_from_csv(fn_csv_Cambridge),ignore_index=True)
	#print "POI dataframe shape: ", df_POI.shape
	#pyplot.scatter(df_POI['X'].values,df_POI['Y'].values,s=1,marker=".")
	#pyplot.show()
	df_POI['command'] = 'GEOADD Boston'
	df_POI['member_num'] = df_POI.index + 10000
	print 'POI dataframe shape: ', df_POI.shape
	print 'df_POI column names: ', df_POI.columns.values
	df_POI.to_csv("POI_commands_to_redis_temp.txt",sep=" ",header=False,index=False,columns=['command','X','Y', 'member_num'])

	with open(r'POI_commands_to_redis_temp.txt', 'r') as infile, open(r'POI_commands_to_redis.txt', 'w') as outfile:
		data = infile.read()
		data = data.replace('"', '')
		outfile.write(data)
		outfile.close()

	os.remove('POI_commands_to_redis_temp.txt')

	# with open(r'POI_commands_to_redis.txt', 'r+') as targetfile:
	# 	data = targetfile.read()
	# 	data = data.replace('"', '')
	# 	targetfile.write(data)
	# 	targetfile.close()


