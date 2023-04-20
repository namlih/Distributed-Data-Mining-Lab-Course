#!/usr/bin/env python3

import sys 
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
 	
conf = SparkConf()
conf.setAppName('vectorize')
sc = SparkContext(conf=conf)

def get_kmers(seq, k):
    kmers = list()
    for i in range(len(seq)-k+1):
        kmers.append(seq[i:i+k])
    return set(kmers)

def vectorize(kmers,features_kmers):
    entries = list()
    for i,kmer in enumerate(features_kmers):
        if kmer in kmers:
            entries.append((i,1.0))
    return SparseVector(len(features_kmers),entries)
    
def parse_occurrences(line):
    line = str(line)
    key = line[1:-1].split(',')[0]
    value = int(line[1:-1].split(',')[1])
    return (key,value)

sequences = sc.textFile("hdfs:///dataset.txt")
#kmersOccurrences = sc.parallelize(sc.textFile("hdfs:///6mers_occurrences.txt").map(lambda line : parse_occurrences(line)).sortBy(lambda x: x[1],False).take(1000))

#kmersIndexed = sc.parallelize(list(zip(range(1000),kmersOccurrences.keys().collect())))
#features_kmers = kmersIndexed.values().collect()
features_kmers = ['GHMINV','GVKNTA','SPPRVQ','MLDDGS','MAFSAE','MSIIGA']
data = sequences.map(lambda line: get_kmers(line,6)).map(lambda kmers: vectorize(kmers,features_kmers)).map(lambda vec: LabeledPoint(1.0,vec))
print(data.collect())

#(trainingData, testData) = data.randomSplit([0.7, 0.3])
#model = RandomForest.trainClassifier(trainingData=trainingData,numClasses=2,numTrees=3)
