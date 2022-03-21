from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand

# To avoid: Exception: Python in worker has different version than that in driver , 
# PySpark cannot run with different minor versions.
# Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# MAP AND REDUCE FUNCTIONS


def count_words_doc (doc, random_partitioning=False, N_partitions=-1):
    """
    count words in a document (map function)
    args:
        doc: a document as a string
        random_partitioning: if True, the words are partitioned randomly
        N_partitions: number of partitions
    returns:
        a list of (word, count) pairs if random_partitioning is False
        a list of (random_key, (word, count)) pairs if random_partitioning is True
    """
    # word count per document
    word_counts={}
    # split the document into words
    for w in doc.split():
        if w not in word_counts.keys(): # if new word
            word_counts[w]=1
        else: # if word already exists
            word_counts[w]+=1

    # partitioning check
    if random_partitioning and N_partitions>0:
        # random partitioning
        partition_id = rand.randint(0, N_partitions-1)
        # assign a random key to the pair (word, word_counts)
        # return (random_key, (word, word_counts))
        return (partition_id, [(k, word_counts[k]) for k in word_counts.keys()])
    elif random_partitioning and N_partitions<=0:
        raise Exception("Number of partitions must be a positive integer")
    elif not random_partitioning and N_partitions>0:
        raise Exception("Random partitioning must be True to perform this operation")
    elif not random_partitioning and N_partitions<=0:
        # return a list of (word, count) tuples
        return [(k, word_counts[k]) for k in word_counts.keys()]


# MAPREDUCE ALGORITHMS

def one_round_wc (docs):
    """
    one round of word count, does not implement random partitioning
    args:
        docs: an RDD of documents
    returns:
        an RDD of (word, count) pairs
    """
    # map(count_words_doc, docs) is an RDD of (word, count) pairs
    # flatMap(count_words_doc, docs) is an RDD of (word, count) pairs
    # reduceByKey(lambda x, y: x+y) is an RDD of (word, count) pairs
    return docs.flatMap(count_words_doc).reduceByKey(lambda x, y: x+y)

