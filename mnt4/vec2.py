#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.evaluation import MulticlassMetrics


from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.mllib.tree import DecisionTree
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


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




def getPredictionsLabels(model, test_data):
   predictions = model.predict(test_data.map(lambda r: r.features))
   return predictions.zip(test_data.map(lambda r: r.label))
def printMetrics(predictions_and_labels):
   metrics = MulticlassMetrics(predictions_and_labels)
   print('Precision of True '+str(metrics.precision(1)))
   print('Precision of False'+str(metrics.precision(0)))
   print('Recall of True  '+str(metrics.recall(1)))
   print('Recall of False   '+str(metrics.recall(0)))
   print('F-1 Score         '+str(metrics.fMeasure()))
   print('Confusion Matrix\n'+str(metrics.confusionMatrix().toArray()))






sequences = sc.textFile("hdfs:///dataset.txt")
six_mers = sequences.filter(lambda line:line.split(',')[2] == '1').flatMap(lambda line: get_kmers(line.split(',')[0],6)).map(lambda kmer: (kmer, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False)

#sample_param = 200000.0 / float(six_mers.count())
features = six_mers.take(1000)
#features += six_mers.sample(False, sample_param, 1).collect()
#features = list(set(features))

data = sequences.map(lambda line: (get_kmers(line.split(',')[0],6),line.split(',')[2])).map(lambda line: (vectorize(line[0],features), line[1])).map(lambda line: LabeledPoint(float(line[1]),line[0]))
#data.saveAsTextFile("hdfs:///vectorized_dataset6.txt")




(trainingData, testData) = data.randomSplit([0.8, 0.2])
#model = RandomForest.trainClassifier(data=trainingData,categoricalFeaturesInfo={},numClasses=2,numTrees=100,maxDepth=6)
model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5, 
maxBins=32)
#testDataFeatures = testData.map(lambda data_point:data_point.features)






predictions = model.predict(testData.map(lambda data_point:data_point.features)).collect()
labels = testData.map(lambda data_point: data_point.label).collect()
metrics = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions, labels))))

predictions_tr = model.predict(trainingData.map(lambda data_point:data_point.features)).collect()
labels_tr = trainingData.map(lambda data_point: data_point.label).collect()
metrics_tr = BinaryClassificationMetrics(sc.parallelize(list(zip(predictions_tr, labels_tr))))

sc.parallelize([('ROC_training', metrics_tr.areaUnderROC), ('PR_training', metrics_tr.areaUnderPR),('ROC_test', metrics.areaUnderROC), ('PR_test', metrics.areaUnderPR)]).saveAsTextFile("hdfs:///random_forest_metrics_15.txt")
