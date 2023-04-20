#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel

conf = SparkConf()
conf.setAppName('NaiveBayes')
sc = SparkContext(conf=conf)
mer = 3

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

def train_pred(param):
    model = NaiveBayes.train(trainData, param)
    predictions = model.predict(testData.map(lambda data_point:data_point.features)).collect()
    labels = testData.map(lambda data_point: data_point.label).collect()
    predictions_float = [float(i) for i in predictions]
    labels_float = [float(i) for i in labels]
    metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions_float, labels_float))))
    predictions_tr = model.predict(trainData.map(lambda data_point:data_point.features)).collect()
    labels_tr = trainData.map(lambda data_point: data_point.label).collect()
    predictions_tr_float = [float(i) for i in predictions_tr]
    labels_tr_float = [float(i) for i in labels_tr]
    metrics_tr = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions_tr_float, labels_tr_float))))
    return [('ROC_training', metrics_tr.areaUnderROC), ('PR_training', metrics_tr.areaUnderPR),('ROC_test', metrics.areaUnderROC), ('PR_test', metrics.areaUnderPR)]

#WARNING: for class ratio 1:1 use dataset, for class ratio 1:5 use dataset2
sequences = sc.textFile("hdfs:///traindata")
six_mers = sequences.filter(lambda line:line.split(',')[2] == '1').flatMap(lambda line: get_kmers(line.split(',')[0],mer)).map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)

#sample_param = 200000.0 / float(six_mers.count())
features = six_mers.take(1000)
#features += six_mers.sample(False, sample_param, 1).collect()
#features = list(set(features))

trainData = sequences.map(lambda line: (get_kmers(line.split(',')[0],mer),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))
#data.saveAsTextFile("hdfs:///vectorized_dataset6.txt")

test_seq = sc.textFile("hdfs:///testdata")
testData=test_seq.map(lambda line: (get_kmers(line.split(',')[0],mer),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))

train_pred(1.0)

#sc.parallelize([('ROC_training', metrics_tr.areaUnderROC), ('PR_training', metrics_tr.areaUnderPR),('ROC_test', metrics.areaUnderROC), ('PR_test', metrics.areaUnderPR)]).saveAsTextFile("hdfs:///"+args(1))
