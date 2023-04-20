from pyspark import SparkContext

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
	sc = SparkContext("local", "primes")
	# Create an RDD of numbers from 0 to 1,000,000
	nums = sc.parallelize(range(1000000))
	# Compute the number of primes in the RDD
	print(nums.filter(isprime).count())
