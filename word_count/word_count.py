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
    # note that, contrary to the theoretical bg of mapreduce, spark does not force to 
    # use pairs as inpout, indeed we are using a list of strings  

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
        return [(partition_id,(key, word_counts[key])) for key in word_counts.keys()]
    elif random_partitioning and N_partitions<=0:
        raise Exception("Number of partitions must be a positive integer")
    elif not random_partitioning and N_partitions>0:
        raise Exception("Random partitioning must be True to perform this operation")
    elif not random_partitioning and N_partitions<=0:
        # return a list of (word, count) tuples
        return [(k, word_counts[k]) for k in word_counts.keys()]

    
def gather_pairs(pairs, partitioning=False):
    """
    get a list of pairs (word, occurrences) and return a list of pairs (word, count)
    by grouping pairs by the same word
    args:
        pairs: a list of pairs (word, occurrences)
        partitioning: if True, the pairs are considered coming from a random partitioning
    returns:
        a list of pairs (word, count)
    """
    pairs_dict = {}
    # if not partitioning, the pair is only one, so we can work only on that
    # in this case the function is called multiple times, once on each pair
    if not partitioning: pairs = pairs[1] 
    for p in pairs:
        
        word, occurrences = p[0], p[1]
        if word not in pairs_dict.keys():
            pairs_dict[word] = occurrences
        else:
            pairs_dict[word] += occurrences
    return [(key, pairs_dict[key]) for key in pairs_dict.keys()]

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
    # reduceByKey: merge the values for each key using an associative and commutative reduce function.
    return docs.flatMap(count_words_doc).reduceByKey(lambda x, y: x+y)

def two_rounds_wc(docs, N_partitions, groupby_key=False):
    """
    two rounds of word count, implements random partitioning
    args:
        docs: an RDD of documents
        N_partitions: number of partitions
    returns:
        an RDD of (word, count) pairs
    """
    if N_partitions<=0: raise Exception("Number of partitions must be a positive integer")

    # one round of word count
    if groupby_key:
        # we introduce a key in the map 
        wc= (docs.flatMap(lambda x: count_words_doc(x, True, N_partitions)) # MAP PHASE (R1)
                        .groupByKey()                                # SHUFFLE+GROUPING
                        .flatMap(lambda x: gather_pairs(x, False))   # REDUCE PHASE (R1)
                        .reduceByKey(lambda x, y: x + y))             # REDUCE PHASE (R2)
    else:
        # groupby generate a key and grop by that key
        wc = (docs.flatMap(count_words_doc) # MAP PHASE (R1)
                    .groupBy(lambda x: (rand.randint(0,N_partitions-1))) # SHUFFLE+GROUPING
                    .flatMap(gather_pairs)                    # REDUCE PHASE (R1)
                    .reduceByKey(lambda x, y: x + y))         # REDUCE PHASE (R2)    
    return wc

def count_words_partitioning(docs):
    """
    count words in a document (map function) with mapPartitions
    args:
        docs: an RDD of documents
    returns:
        an RDD of (word, count) pairs
    """
    # word count per document
    wc = (docs.flatMap(count_words_doc) # <-- MAP PHASE (R1)
        .mapPartitions(lambda x: gather_pairs(x, True))    # <-- REDUCE PHASE (R1)
        .groupByKey()                              # <-- SHUFFLE+GROUPING
        .mapValues(lambda x: sum(x)))        # <-- REDUCE PHASE (R2)
    return wc

    