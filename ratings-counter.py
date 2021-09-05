from pyspark import SparkConf, SparkContext
import collections

# telling the program that were only going to run on the local box only, not on a cluster 
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# create a spark configuration object, always call it sc
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///scourse/ml-100k/u.data")
# lambda - a small anonymous function that takes any number of arguments but has only one expression. 
# lambdas are very useful in using them as an anonymous function inside another function. 
# the map function takes the lines from the textfile and splits it into different parts based off a delimeter (the white space)
# then it grabs the 3rd (not 2nd, remember 0,1,2) array value and using the lambda function x, maps (or extract) the results 
# to the ratings variable.
ratings = lines.map(lambda x: x.split()[2])
# breaks the ratings down by pair values of how many times they appear, ex: 3, 3, 1, 2, 1 => (3, 2) because 3 apears 2 times
result = ratings.countByValue()

# just sort and display the results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
