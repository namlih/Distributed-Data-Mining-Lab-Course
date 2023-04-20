#!/usr/bin/env python3

import sys 
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics
 	
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
    
sequences = sc.textFile("hdfs:///dataset.txt")
six_mers = sequences.flatMap(lambda line: get_kmers(line.split(',')[0], 6)).map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)

sample_param = 100000.0 / float(six_mers.count())
features = six_mers.take(100000)
features += six_mers.sample(False, sample_param, 1).collect()
features = list(set(features))

data = sequences.map(lambda line: (get_kmers(line.split(',')[0],6),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))
data.saveAsTextFile("hdfs:///vectorized_dataset.txt")

(trainingData, testData) = data.randomSplit([0.7, 0.3])
model = RandomForest.trainClassifier(trainingData=trainingData,numClasses=2,numTrees=3)
predictions = testData.map(lambda data_point: model.predict(data_point.features)).collect()
labels = testData.map(lambda data_point: data_point.label).collect()
metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions, labels))))

sc.parallelize([('ROC', metrics.areaUnderROC), ('PR', metrics.areaUnderPR)]).saveAsTextFile("hdfs:///random_forest_metrics_1.txt")
