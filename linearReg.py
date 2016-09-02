'''
Created on Nov 10, 2015

@author: rumit
'''
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin.csv

# import the necessary packages 
from pyspark import SparkContext
import sys
import numpy as np
from numpy.linalg import inv

# function to get the y points from the file  
def parsePointY(line):
        values = [float(x) for x in line.split(',')]
        return (values[0])

# function to get the x points from the file
def parsePointX(line):
        values = [float(x) for x in line.split(',')]
        return (values[1:])

def linearreg(x_lines,y_lines):
    Key_A = x_lines.map(lambda a:("KeyA",(np.array(a).reshape(len(a),1) * np.transpose(np.array(a).astype('float')).reshape(1,len(a)))))
    A=Key_A.reduceByKey(lambda x,y : x+y).values()
    Key_B = y_lines.map(lambda b:("KeyB",(np.array(b)[1:len(b)].reshape(len(b)-1,1) * np.array(b)[0].reshape(1,1))))
    B=Key_B.reduceByKey(lambda x,y : x+y).values()
    A= np.array(A.collect()).squeeze()
    B= np.array(B.collect())
    A_inverse = inv(A)
    weight = np.dot(A_inverse,B)
    return weight

# checking for the proper input arguments if two arguments are not passed exit the code 
if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

# initialize the spark context to read the input csv file and store it in yxinputfile RDD
sc = SparkContext(appName="LinearRegression")
yxinputFile = sc.textFile(sys.argv[1])
yxlines = yxinputFile.map(lambda line: line.split(','))
              
# getting the x and y values through parsepoint function 
ylines = yxinputFile.map(parsePointY)
xlines = yxinputFile.map(parsePointX)

# creating the array using the numpy package 
x = np.array(xlines.collect()).astype('float')
y = np.array(ylines.collect()).astype('float')

# adding a bias term to the x data points
x_bias  = np.c_[np.ones(len(x)), x]

x_lines = sc.parallelize(x_bias)
y_lines = sc.parallelize(np.c_[y,x_bias])

# pass this new x & y values to linearreg function
beta = linearreg(x_lines, y_lines)

print("*************OUTPUT**************")
print ("beta:")
for weights in beta:
    print weights

sc.stop()