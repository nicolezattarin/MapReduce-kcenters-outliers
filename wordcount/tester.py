from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
from word_count import*
import time

# To avoid: Exception: Python in worker has different version than that in driver , 
# PySpark cannot run with different minor versions.
# Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    # see https://spark.apache.org/docs/2.1.0/programming-guide.html#initializing-spark

    assert len(sys.argv) == 3, "Usage: python WordCountExample.py <number of file partitions> <file_name>"
    # SPARK SETUP
    print("Setting up SparkContext...")
    conf = SparkConf().setAppName('WordCountExample').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Read number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "Number of partitions must be a positive integer"
    K = int(K)
    print ("Number of partitions required = ", K)

    # Read input file 
    data_path = sys.argv[2]
    print ("data_path: ", data_path)
    assert os.path.isfile(data_path), "File or folder not found"

    # Read input file and subdivide it into K random partitions 
    # textFile Read a text file from local file system and return it as an RDD of Strings.
    # .cache() Persist this RDD with the default storage level

    # docs is my RDD with data
    docs = sc.textFile(data_path, minPartitions=K).cache() #min k partitions
    docs.repartition(numPartitions=K) # exactly K partitions
    print("Number of partitions = ", docs.getNumPartitions())
    print("Document loaded")

    # global variables
    numdocs = docs.count()
    print("Number of documents = ", numdocs)

    # ONE ROUND, NO PARTITIONING
    t = time.time()
    counts = one_round_wc(docs).count()
    print("\n\nONE ROUND NO PARTITIONING")
    print("NUMBER OF WORDS = ", counts)
    print("TIME = ", time.time() - t)


    
if __name__ == '__main__':
    main()