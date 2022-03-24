import numpy as np
from pyspark import SparkContext, SparkConf
import sys, os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def filter(line, S):
    if len(line) != 8: raise ValueError ("Wrong number of fields")
    if S != "all":
        if int(line[3])>0 and line[7]==S:
            return True
        else:
            return False
    else:
        if int(line[3])>0:
            return True
        else:
            return False

def count_popularity(partitionData):
    for row in partitionData:
        yield (row[0],1)

def main():
    # Design a program which receives in input, as command-line (CLI) arguments:
    # an integer K, an integer H, a string S, and a path to the file storing the dataset
    if len(sys.argv) != 5: raise ValueError ("Usage: <K> <H> <S> <path_to_dataset>")
    if not sys.argv[1].isdigit(): raise ValueError ("K must be a positive integer")
    if not sys.argv[2].isdigit(): raise ValueError ("H must be a positive integer")
    K = int(sys.argv[1])
    H = int(sys.argv[2])
    S = sys.argv[3]
    path = sys.argv[4]
    print('K =', K, 'H =', H, 'S =', S, 'path =', path)
    if not os.path.isfile(path): raise ValueError ("File or folder not found")

    # setup spark context 
    print("\n\nSetting up SparkContext...")
    conf = SparkConf().setAppName('DataAnalysis').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Read the input file into an RDD of strings called rawData
    # (each 8-field row is read as a single string), and subdivides it into K partitions
    rawData = sc.textFile(path, minPartitions=K).cache() 
    rawData.repartition(numPartitions=K) 
    print("\n\nNumber of partitions = ", rawData.getNumPartitions())
    print("Document loaded")

    # prints the number of rows read from the input file 
    nrows = rawData.count() #counting number of strings in RDD
    print("\n\nNumber of rows = ", nrows)

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(rawData.take(10))

    # Transform rawData into an RDD of (String, Integer) pairs called productCustomer, 
    # which contains all distinct pairs (P,C) such that rawData contains one or more 
    # strings whose constituent fields satisfy the following conditions : 
    # ProductID=P and CustomerID=C, Quantity>0, and Country=S. If S="all", 
    # no condition on Country is applied. 

    # map Return a new RDD by applying a function to each element of this RDD.
    # filter Return a new RDD containing only the elements that satisfy a predicate.
    # we select positive quantities and countries that match S
    productCustomer = rawData.map(lambda line: line.split(",")) \
                          .filter(lambda line: filter(line, S)) \
                          .map(lambda line: (line[1], line[6])) # (ProductID, CustomerID)

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(productCustomer.take(10))

    # print the number of pairs in the RDD. 
    print("\n\nProduct-Customer Pairs = ", productCustomer.count())

    # transform productCustomer into an RDD of (String,Integer) pairs called productPopularity1 which, 
    # for each product ProductID contains one pair (ProductID, Popularity), 
    # where Popularity is the number of DISTINCT customers from Country S (or from all countries if S="all") 
    # that purchased a positive quantity of product ProductID. IMPORTANT: in this case it is 
    # safe to assume that the amount of data in a partition is small enough to be gathered together.

    # distinct: Return a new RDD containing the distinct elements in this RDD.
    productPopularity1 = productCustomer.distinct()
    productPopularity1 = productPopularity1.mapPartitions(count_popularity)\
                                            .groupByKey()\
                                            .mapValues(sum)

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nDistinct ProductID (1) =", productPopularity1.count())

    # Repeats the operation of the previous point using a combination of map/mapToPair 
    # and reduceByKey methods (instead of mapPartitionsToPair/mapPartitions) and calling the resulting RDD productPopularity2.
    productPopularity2 = productCustomer.distinct()
    productPopularity2 = productPopularity2.map(lambda line: (line[0], 1))\
                                            .groupByKey()\
                                            .map(lambda line: (line[0], sum(line[1])))

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nDistinct ProductID (2) =", productPopularity1.count())

    # Saves in a list and prints the ProductID and Popularity of the H products 
    # with highest Popularity. (Extracts these data from productPopularity1. )
    if H > productPopularity1.count():
        raise ValueError ("H is greater than the number of distinct products")

    if H > 0:
        print("\n\nH =", H, "(1) products with highest Popularity:")
        productPopularity1 = productPopularity1.sortBy(lambda line: line[1], ascending=False)
        productPopularity1 = productPopularity1.take(H)
        for i in range(H):
            print(productPopularity1[i][0], ":", productPopularity1[i][1])
        print ("\n\nH =", H, "(2) products with highest Popularity:")
        productPopularity2 = productPopularity2.sortBy(lambda line: line[1], ascending=False)
        productPopularity2 = productPopularity2.take(H)
        for i in range(H):
            print(productPopularity2[i][0], ":", productPopularity2[i][1])
    
    # (This step, for debug purposes, is executed only if H=0) Collects all pairs of productPopularity1 
    # into a list and print all of them, in increasing lexicographic order of ProductID. 
    # Repeats the same thing using productPopularity2.
    if H == 0:
        productPopularity1 = sorted(productPopularity1.collect(), key = lambda x: x[0])
        productPopularity2 = sorted(productPopularity2.collect(), key = lambda x: x[0])
        print("\n\nAll ProductID (1):\n", productPopularity1)
        print("\n\nAll ProductID (2):\n", productPopularity2)
    print('\n')

if __name__ == '__main__':
    main()