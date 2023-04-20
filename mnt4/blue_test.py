#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics

conf = SparkConf()
conf.setAppName('RandomForestModel')
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
    return SparseVector(len(features_kmers),entries)

mer = 3
num_trees = 150
max_depth = 15

feature_size = 1000
name_of_the_file = "hdfs:///random_forest_parameters_1000_150_15.txt"


#WARNING: for class ratio 1:1 use dataset, for class ratio 1:5 use dataset2
sequences = sc.textFile("hdfs:///traindata")
six_mers = sequences.filter(lambda line:line.split(',')[2] == '1').flatMap(lambda line: get_kmers(line.split(',')[0],mer)).map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)

#sample_param = 200000.0 / float(six_mers.count())
features = six_mers.take(feature_size)
#features += six_mers.sample(False, sample_param, 1).collect()
#features = list(set(features))

trainData = sequences.map(lambda line: (get_kmers(line.split(',')[0],mer),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))
#data.saveAsTextFile("hdfs:///vectorized_dataset6.txt")

test_seq = sc.textFile("hdfs:///testdata")
testData=test_seq.map(lambda line: (get_kmers(line.split(',')[0],mer),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))

#(trainingData, testData) = data.randomSplit([1.8, 0.2])
model = RandomForest.trainClassifier(data=trainData,categoricalFeaturesInfo={},numClasses=2,numTrees=num_trees,maxDepth=max_depth)
#testDataFeatures = testData.map(lambda data_point:data_point.features)
predictions = model.predict(testData.map(lambda data_point:data_point.features)).collect()
labels = testData.map(lambda data_point: data_point.label).collect()
metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions, labels))))

predictions_tr = model.predict(trainData.map(lambda data_point:data_point.features)).collect()
labels_tr = trainData.map(lambda data_point: data_point.label).collect()
metrics_tr = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions_tr, labels_tr))))

sc.parallelize([('ROC_training', metrics_tr.areaUnderROC), ('PR_training', metrics_tr.areaUnderPR),('ROC_test', metrics.areaUnderROC), ('PR_test', metrics.areaUnderPR)]).saveAsTextFile(name_of_the_file)

