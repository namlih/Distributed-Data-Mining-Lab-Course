#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.classification import MultilayerPerceptronClassifier

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("mlp model") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#conf = SparkConf()
#conf.setAppName('MultilayerPerceptronModel')
#sc = SparkContext(conf=conf)
sc = spark.sparkContext

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

sequences = sc.textFile("hdfs:///dataset.txt")
six_mers = sequences.flatMap(lambda line: get_kmers(line.split(',')[0], 6)).map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)

features = six_mers.take(4096)

data = sequences.map(lambda line: (get_kmers(line.split(',')[0],6),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: (float(line[1]),line[0]))

(trainingData, testData) = data.randomSplit([0.7, 0.3])

training_df = spark.createDataFrame(trainingData.collect(),["label","features"])
test_df = spark.createDataFrame(testData.map(lambda line: (line[1])).collect(),["features"])

mlp = MultilayerPerceptronClassifier(maxIter=100,layers=[1000,100,10],seed=123)
model = mlp.fit(training_df)
predictions = [float(row.prediction) for row in model.transform(test_df).select("prediction").collect()]
labels = testData.map(lambda data_point: data_point.label).collect()
metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions, labels))))

#predictions_tr = model.predict(trainingData.map(lambda data_point:data_point.features)).collect()
#labels_tr = trainingData.map(lambda data_point: data_point.label).collect()
#metrics_tr = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions_tr, labels_tr))))

sc.parallelize([('ROC_test', metrics.areaUnderROC), ('PR_test', metrics.areaUnderPR)]).saveAsTextFile("hdfs:///mlp_metrics_1.txt")
