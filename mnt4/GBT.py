#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, GradientBoostedTrees
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.classification import GBTClassifier

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

#WARNING: for class ratio 1:1 use dataset, for class ratio 1:5 use dataset2
sequences = sc.textFile("hdfs:///dataset.txt")
six_mers = sequences.filter(lambda line:line.split(',')[2] == '1').flatMap(lambda line: get_kmers(line.split(',')[0],3)).map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)

#sample_param = 200000.0 / float(six_mers.count())
features = six_mers.take(1000)
#features += six_mers.sample(False, sample_param, 1).collect()
#features = list(set(features))

data = sequences.map(lambda line: (get_kmers(line.split(',')[0],3),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))
#data.saveAsTextFile("hdfs:///vectorized_dataset6.txt")

(trainingData, testData) = data.randomSplit([0.8, 0.2])
model = GradientBoostedTrees.trainClassifier(data=trainingData,categoricalFeaturesInfo={},numIterations=5)
#testDataFeatures = testData.map(lambda data_point:data_point.features)
predictions = model.predict(testData.map(lambda data_point:data_point.features)).collect()
labels = testData.map(lambda data_point: data_point.label).collect()
metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions, labels))))

predictions_tr = model.predict(trainingData.map(lambda data_point:data_point.features)).collect()
labels_tr = trainingData.map(lambda data_point: data_point.label).collect()
metrics_tr = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions_tr, labels_tr))))

sc.parallelize([('ROC_training', metrics_tr.areaUnderROC), ('PR_training', metrics_tr.areaUnderPR),('ROC_test', metrics.areaUnderROC), ('PR_test', metrics.areaUnderPR)]).saveAsTextFile("hdfs:///gbt_metrics_1.txt")
