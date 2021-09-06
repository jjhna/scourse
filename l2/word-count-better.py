# re = regular expression
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    # r'\W' means that you want to break up the text by words, also tell the function that it may have some unicode in it and 
    # set the text to lowercase
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///scourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
