#!/usr/bin/env python3

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.linalg import Vectors

spark = SparkSession \
    .builder \
    .appName("chisq model") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

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
    return Vectors.sparse(len(features_kmers),entries)

sequences = sc.textFile("hdfs:///dataset.txt")


"""six_mers = sequences.filter(lambda line:line.split(',')[2] == '1') \
                    .flatMap(lambda line: get_kmers(line.split(',')[0],3)) \
                    .map(lambda kmer:(kmer,1)) \
                    .reduceByKey(lambda a,b:a+b) \
                    .sortBy(lambda x: x[1], False)"""


#sc.parallelize([six_mers.count()]).saveAsTextFile("hdfs:///six_mers_count.txt")
#features = six_mers.take(500000)

three_mers = sc.textFile("hdfs:///3mers_adjusted_previouscode").map(lambda line: (line[1:4],int(line.split(',')[1][:-1]))).sortBy(lambda x: x[1], False)
features = three_mers.take(500)


data = sequences.map(lambda line: (get_kmers(line.split(',')[0],3),line.split(',')[2])) \
                .map(lambda line: (vectorize(line[0],features), line[1])) \
                .map(lambda line: (float(line[1]),line[0])).collect()

df = spark.createDataFrame(data,["label","features"])

selector = ChiSqSelector(numTopFeatures = 200, featuresCol="features",outputCol="selected_features",labelCol="label")

result = selector.fit(df).transform(df)

selected_features = [row.selected_features for row in result.select('selected_features').collect()]

sc.parallelize(selected_features).saveAsTextFile("hdfs:///chisq_selected_features_3mers_top200.txt")



