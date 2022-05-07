# Big Data Computing with Spark
Repository based on the course [Big data computing @ Unipd](https://didattica.unipd.it/off/2021/LM/IN/IN2371/002PD/INP7079233/G2GR2).

# Installation 
To install pyspark on your local machine, please refer to the [official installation guide](https://spark.apache.org/docs/latest/installation.html) or [specific python instructions](http://www.dei.unipd.it/~capri/BDC/PythonInstructions.html).

# Overview
### Basic example: word count
We provide a basic example of word count in Spark: given a text file and a fixed number of partitions, we implement different algorithms to count the number of words in the text file. See the [README](https://github.com/nicolezattarin/Big-Data-Computing/tree/main/word_count) for a more detailed description of the example.

### First MapReduce example: 
In [basic_data_processing](https://github.com/nicolezattarin/Big-Data-Computing/tree/main/basic_data_processing) we develop a spark program to analyze a dataset of an online retailer which contains several transactions made by customers, where a transaction represents several products purchased by a customer. All details are specified in the corresponding folder.
