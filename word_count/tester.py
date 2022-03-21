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
    print("\n\nSetting up SparkContext...")
    conf = SparkConf().setAppName('WordCountExample').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Read number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "\n\nNumber of partitions must be a positive integer"
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
    print("TIME = {:.2f}".format(time.time() - t))

    # TWO ROUNDS, groupkey
    t = time.time()
    counts = two_rounds(docs, K, groupby_key=True).count()
    print("\n\nTWO ROUND RANDOM GR")
    print("NUMBER OF WORDS = ", counts)
    print("TIME = {:.2f}".format(time.time() - t))

    # TWO ROUNDS, SHUFFLE WITH KEY
    t = time.time()
    counts = two_rounds(docs, K, groupby_key=False).count()
    print("\n\nTWO ROUND RANDOM NO GROUPKEY")
    print("NUMBER OF WORDS = ", counts)
    print("TIME = {:.2f}".format(time.time() - t))

    # TWO ROUNDS, PARTITIONING
    t = time.time()
    counts = count_words_partitioning(docs).count()
    print("\n\n PARTITIONING")
    print("NUMBER OF WORDS = ", counts)
    print("TIME = {:.2f}".format(time.time() - t))

    # GET LIST OF PAIRS (using last implementation)
    wp = count_words_partitioning(docs)
    print ("\n\nPAIRS as RDD")
    print (wp.take(10))

    print ("\n\nPAIRS as LIST")
    wp_list = wp.collect()
    print (wp_list[:10])

    print ("\n\nPAIRS as DICT (sorted)")
    wp_dict = wp.collectAsMap()
    sorted_dict = [(k, wp_dict[k]) for k in sorted(wp_dict, key=wp_dict.get, reverse=True)]
    print (sorted_dict[:10])


    # AVERAGE LENGTH OF WORDS
    print ("\n\nAVERAGE LENGTH OF WORDS = ", wp.map(lambda x: len(x[0])).mean())


if __name__ == '__main__':
    main()