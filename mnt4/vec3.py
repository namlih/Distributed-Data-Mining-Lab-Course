#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.ml.linalg import Vectors

conf = SparkConf()
conf.setAppName('ZeroFeatureVectorsforDifferentKmers')
sc = SparkContext(conf=conf)

def get_kmers(seq, k):
    kmers = list()
    for i in range(len(seq)-k+1):
        kmers.append(seq[i:i+k])
    return set(kmers)

def vectorize(kmers,features_kmers):
    entries = list()
    for i,kmer in enumerate(features_kmers):
        if str(kmer[0]) in kmers:
            entries.append((i,1.0))
    return Vectors.sparse(len(features_kmers),entries)

sequences = sc.textFile("hdfs:///dataset.txt")

results = list()
feature_sizes = [100,200,500,1000,2000,5000]
for k in range(3,6):
    kmers = sequences.flatMap(lambda line: get_kmers(line.split(',')[0],k)) \
                     .map(lambda kmer: (kmer,1)) \
                     .reduceByKey(lambda a,b:a+b) \
                     .sortBy(lambda x: x[1], False)
    for feature_size in feature_sizes:
        features = kmers.take(feature_size)
        data = sequences.map(lambda line: get_kmers(line.split(',')[0],k)) \
                        .map(lambda kmers: vectorize(kmers,features))
    
        results.append(data.filter(lambda vec: vec.numNonzeros() == 0).count())
sc.parallelize(results).saveAsTextFile("hdfs:///zerofeaturevectorscount1.txt")

