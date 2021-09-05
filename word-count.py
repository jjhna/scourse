from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# difference between map and flatmap:
# map can only do a 1 to 1 transfer from 1 element to a new element
# flatmap however can create multiple new elements from 1 element. So it can split a string into a char list or numbers to an int array
input = sc.textFile("file:///scourse/book.txt")
# the split function splits the text by whitespace while flatmap maps those individual text into a list
words = input.flatMap(lambda x: x.split())
# get a count of how many times each unique values occurs
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
