# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 15:28:00 2020

@author: Frank
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    # the "r" stand for read only, the rest is gibberish
    with codecs.open("D:/scourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            # the dataset is delimetered by a pipe character
            fields = line.split('|')
            # we then create a dictionary where we map the movieID by its movie string name
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# ImportantNOTE: broadcast is a function that allows the user to display movie names instead of ID numbers
# It ships off all the executors to the drivers, so it collects all the data and broadcasts the data back to the executors.
# now the dictionary loadMovieNames() is then broadcasted to all the cluster notifying them that this data is avaialble
nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///scourse/ml-100k/u.data")
# create both a column for movie ID and a new column called count
movieCounts = moviesDF.groupBy("movieID").count()

# Create a user-defined function to look up movie names from our broadcasted dictionary
# so we just create a function called lookupName that takes in the movieID (int) as a parameter and returns the value of that key in the dictionary
def lookupName(movieID):
    return nameDict.value[movieID]

# .udf - user defined function, creates a new function called lookupNameUDF that can be called and used in SQL
lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
# so first we call the new udf function lookupNameUDF that takes in the movieID and then create a new column called "movieTitle"
# this well then fill up the movieTitle column with the movieID string name, using the withColumn function
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# Sort the results, order that same count column that we made earlier and order it from largest to smallest
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
