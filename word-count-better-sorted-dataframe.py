from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///scourse/book.txt")

# Split using a regular expression that extracts words
# since we are taking words from a text file and nothing organized like a csv file, we can only take from the default column,
# the default name of a column that isn't named is called value, so we utilize the variable inputDF.value
# \\W+ tells the split on the delimiters of whitespaces, periods, and commas, etc.

# .explode is similar to flatmap(), where it creates our text into multiple rows for each word. 
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
# we get a lot of empty words, so we need to remove them
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
# the default amount of results that can be displayed in the cli is 20
# however you can override this by using the .count to show everything
wordCountsSorted.show(wordCountsSorted.count())
