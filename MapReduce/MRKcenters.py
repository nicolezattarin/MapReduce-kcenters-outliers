# Import Packages
from pyspark import SparkConf, SparkContext
import numpy as np
import time
import random
import sys
import math
from kcenters_coreset import strToVector, MR_kCenterOutliers, ComputeObjective

# To avoid: Exception: Python in worker has different version than that in driver , 
# PySpark cannot run with different minor versions.
# Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
import os,sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    """
    args:
        path: A path to a text file containing point set in Euclidean space.
             Each line of the file contains, separated by commas, the coordinates of a point. 
        k: The number of centers to find.
        z: The number of outliers to find.
        L: The number of partitions to use.
    
    """
    # Checking number of cmd line parameters
    assert len(sys.argv) == 5, "Usage: python Homework3.py <filepath> <k> <z> <L>"

    # Initialize variables
    filename = sys.argv[1]
    k = int(sys.argv[2])
    z = int(sys.argv[3])
    L = int(sys.argv[4])
    start = 0
    end = 0

    # Set Spark Configuration
    conf = SparkConf().setAppName('MR k-center with outliers')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # Reads the input points into and RDD of Vector called inputPoints, 
    # subdivided into L partitions, sets the Spark configuration, and prints various statistics.
    start = time.time()
    inputPoints = sc.textFile(filename, L).map(lambda x : strToVector(x)).repartition(L).cache()
    N = inputPoints.count()
    end = time.time()

    # Pring input parameters
    print("File : " + filename)
    print("Number of points N = ", N)
    print("Number of centers k = ", k)
    print("Number of outliers z = ", z)
    print("Number of partitions L = ", L)
    print("Time to read from file: ", str((end-start)*1000), " ms")

    # Solve the problem: find a set of at most k centers with z outliers for the input dataset
    solution = MR_kCenterOutliers(inputPoints, k, z, L)

    # Compute the value of the objective function
    start = time.time()
    objective = ComputeObjective(inputPoints, solution, z)
    end = time.time()
    print("Objective function = ", objective)
    print("Time to compute objective function: ", str((end-start)*1000), " ms")
     

# Just start the main program
if __name__ == "__main__":
    main()

