import numpy as np
from pyspark import SparkContext, SparkConf
import sys, os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def clean_data(line):
    # convert to float numerical values
    datalist = line.split(",")
    return int(datalist[0]),(float(datalist[1]), float(datalist[2]), float(datalist[3]), float(datalist[4]))

def main():
    # initialize Spark
    conf = SparkConf().setAppName('clustering').setMaster('local[*]')
    sc = SparkContext(conf=conf) 

    # read in raw data and partition
    npartitions = 2
    # data is organizes as: Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species
    rawData = sc.textFile("iris.csv", minPartitions=npartitions).cache()
    rawData.repartition(numPartitions=npartitions)

    #filter to keep only: SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm
    rawData = rawData.map(lambda line: clean_data(line))

    #check assignment
    nclusters = 3
    centers = [np.array([4.4,2.9,1.4,0.2]),\
            np.array([6.7,3.0,5.0,1.7]),\
            np.array([5.9,3.0,5.1,1.8])]
    from func import assign
    assigned_set = assign(rawData, centers)

    # DEBUG
    # print(assigned_set.collect())

    # FFT
    from func import FarthestFirstTraversal
    FarthestFirstTraversal(rawData)


if __name__ == '__main__':
    main()