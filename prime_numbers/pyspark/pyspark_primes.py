#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
import sys

def isprime(n):
    """
    check if integer n is a prime
    """
    # make sure n is a positive integer
    n = abs(int(n))
    # 0 and 1 are not primes
    if n < 2:
        return False
    # 2 is the only even prime number
    if n == 2:
        return True
    # all other even numbers are not primes
    if not n & 1:
        return False
    # range starts with 3 and only needs to go up the square root of n
    # for all odd numbers
    for x in range(3, int(n**0.5)+1, 2):
        if n % x == 0:
            return False
    return True
    
if __name__ == '__main__':
	conf = SparkConf()
	conf.setAppName('primes')	
	sc = SparkContext(conf=conf)

	nums = sc.textFile("hdfs:///prime_input/input1.txt")
	
	primeCounts = sc.parallelize([nums.filter(lambda n : isprime(n)).count()])
	primeCounts.saveAsTextFile("hdfs:///pyspark_primes_output1.txt")

