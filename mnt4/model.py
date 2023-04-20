#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics

conf = SparkConf()
conf.setAppName('model')
sc = SparkContext(conf=conf)


data = sc.textFile("hdfs:///vectorized_dataset.txt")

(trainingData, testData) = data.randomSplit([0.7, 0.3])
model = RandomForest.trainClassifier(data=trainingData,categoricalFeaturesInfo={},numClasses=2,numTrees=3)
predictions = testData.map(lambda data_point: model.predict(data_point.features)).collect()
labels = testData.map(lambda data_point: data_point.label).collect()
metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions, labels))))

sc.parallelize([('ROC', metrics.areaUnderROC), ('PR', metrics.areaUnderPR)]).saveAsTextFile("hdfs:///random_forest_metrics_1.txt")
