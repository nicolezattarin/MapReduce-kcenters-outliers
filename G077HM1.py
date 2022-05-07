import numpy as np
from pyspark import SparkContext, SparkConf
import sys, os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# receives in input, as arguments, 2 integers K and H, a string S, 
# and a path to the file storing the dataset

def filterData(line, S):
    # Filter by Country and quantity
    if S.lower() == 'all' and int(line[3]) > 0: return True
    elif S.lower() == line[-1].lower() and int(line[3]) > 0: return True
    else: return False   

def main():
    if len(sys.argv) != 5: raise ValueError ("Usage: <K> <H> <S> <path_to_dataset>")
    if not sys.argv[1].isdigit(): raise ValueError ("K must be a positive integer")
    if not sys.argv[2].isdigit(): raise ValueError ("H must be a integer >= 0")
    K = int(sys.argv[1]) # Number of partitions
    H = int(sys.argv[2]) # if H>0, we take H products with highest Popularity
    S = sys.argv[3] # name of the country
    path = sys.argv[4] # path to the file storing the dataset
    if not os.path.isfile(path): raise ValueError ("File or folder {} not found".format(path))

    # Initialize PySpark
    conf = SparkConf().setAppName('G0077HM1').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # Read in raw data and partition
    rawData = sc.textFile(path, minPartitions=K).cache()
    rawData.repartition(numPartitions=K)

    # prints the number of rows read from the input file
    nrows = rawData.count() #counting number of strings in RDD
    print("\n\nNumber of rows = ", nrows)

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(rawData.take(10))
    # print((rawData.collect()[3]))
    
    # Filter by Country and quantity 
    rawData = rawData.map(lambda line: line.split(",")).filter(lambda line: filterData(line, S))
    # up to now each row is a tuple (product, customer)

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(rawData.take(10))
    
    # DEBUG - prints the number of rows read from the input file, filtered
    # print("\n\nNumber of rows in the filtered = ", rawData.count())

    productCustomer = rawData.map(lambda line: ((line[1], line[6]), 1))\
                            .reduceByKey(lambda x, y: x)\
                            .map (lambda pair: pair[0])

    print("\n\nNumber of distinct products = ", productCustomer.count())

    # DEBUG - prints the first 10 rows of the RDD
    # test = rawData.map(lambda line: (line[1], line[6])).distinct()
    # print("\n\nNumber of distinct products (correct one) = ", test.count())

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(productCustomer.take(10))

    # transform productCustomer into an RDD of (String, Integer) pairs
    # for each product ProductID contains one pair (ProductID, Popularity), 
    # where Popularity is the number of distinct customers 

    # using mapPartitions, groupbykey and mapValues
    def f(partition): 
        for p in partition: yield (p[0], 1)
    productPopularity1 = productCustomer.mapPartitions(f).groupByKey().mapValues(len)

    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(productPopularity1.sortBy(lambda x: x[1], ascending=False).take(10))
    
    # using map and reduceByKey
    productPopularity2 = productCustomer.map(lambda pair: (pair[0], 1)).reduceByKey(lambda x,y: x+y)
    # DEBUG - prints the first 10 rows of the RDD
    # print("\n\nFirst 10 rows of the RDD:")
    # print(productPopularity2.sortBy(lambda x: x[1], ascending=False).take(10))

    # Print the top H products with highest Popularity
    if H>0: 
        topHproducts1 = productPopularity1.sortBy(lambda x: x[1], ascending=False).take(H)
        print(f'\n\nThe top {H} products are {topHproducts1}')

    # Collect all pairs into a list and print all of them, 
    # in increasing lexicographic order of ProductID
    if H==0:
        productPopularity_list1 = productPopularity1.sortByKey().collect()
        print(f'\n\nproductPopularity1 products are {productPopularity_list1}')
        productPopularity_list2 = productPopularity2.sortByKey().collect()
        print(f'\n\nproductPopularity2 products are {productPopularity_list2}')

if __name__ == "__main__":
    main()