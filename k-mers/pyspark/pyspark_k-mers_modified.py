#!/usr/bin/env python 

import sys
from nltk import ngrams
from pyspark import SparkContext, SparkConf

conf = SparkConf()
conf.setAppName('kmers')
sc = SparkContext(conf=conf)


def simple(line,k):
    kmers = list()
    kmers.append(ngrams(line,k))
    return kmers


kmers = sc.textFile("hdfs:///kmers_input/small.txt").flatMap(lambda line: simple(line,3))
kmersOccurrences = kmers.map(lambda kmer: (kmer, 1)).reduceByKey(lambda a,b:a +b).distinct
kmersOccurrences.saveAsTextFile("hdfs:///dna/output_k-mers3.txt")
