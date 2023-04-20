#!/usr/bin/env python3

import sys
import re
from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.evaluation import BinaryClassificationMetrics

conf = SparkConf()
conf.setAppName('converter')
sc = SparkContext(conf=conf)

def converter(line):

    splitted_line = re.split(r'\[(.*?)\]|[(|)]', line)
    c = float(splitted_line[2].strip(','))
    d = int(splitted_line[4].strip(','))
    e = splitted_line[5].split(',')
    e = [int(x) for x in e]
    f = splitted_line[7].split(',')
    f = [float(x) for x in f]
    new_vector = SparseVector(d, e, f)
    new_point = LabeledPoint(c, new_vector)
    return new_point



raw_data = sc.textFile("hdfs:///vectorized_dataset.txt")
data = raw_data.map(lambda line: converter(line))
