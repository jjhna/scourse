import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///scourse/book.txt")
words = input.flatMap(normalizeWords)

# we need to use reduce by keys to reduce the amount of times any duplicates or copies show up
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# we need to flip the key, value pair from (word, count) to (count, word) 
# then we use sortbykey to sort all the values by the key from 1 to max num
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
