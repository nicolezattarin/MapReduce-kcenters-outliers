# Big Data Computing with Spark
Repository based on the course [Big data computing @ Unipd](https://didattica.unipd.it/off/2021/LM/IN/IN2371/002PD/INP7079233/G2GR2).

# Installation 
To install pyspark on your local machine, please refer to the [official installation guide](https://spark.apache.org/docs/latest/installation.html) or [specific python instructions](http://www.dei.unipd.it/~capri/BDC/PythonInstructions.html).

# Overview
### Basic example: word count
We provide a basic example of word count in Spark: given a text file and a fixed number of partitions, we implement different algorithms to count the number of words in the text file. See the [README](https://github.com/nicolezattarin/Big-Data-Computing/tree/main/word_count) for a more detailed description of the example.

### Basic data processing: 
In [basic_data_processing](https://github.com/nicolezattarin/Big-Data-Computing/tree/main/basic_data_processing) we develop a spark program to analyze a dataset of an online retailer which contains several transactions made by customers, where a transaction represents several products purchased by a customer. All details are specified in the corresponding folder.

### kcenters with outliers:
In [outliers_kcenters](https://github.com/nicolezattarin/Big-Data-Computing/tree/main/outliers_kcenters) we develop a sequential algorithm to find the centers of clusters in presence of outliers.
Given a set P of points and two integers k and z, the model finds the k centers of clusters in P, with at most z outliers. A method to compute the loss function is also provided.
A 2 dimensional example follows:
<p align="center">
  <img src="outliers_kcenters/kcenter_k3_z1_test.png" width="400" />
</p>

In [outliers_kcenters_optimized](https://github.com/nicolezattarin/Big-Data-Computing/tree/main/outliers_kcenters_optimized) we provide the optimized version of the algorithm, which makes use od dynamic programming.


