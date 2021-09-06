from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///scourse/fakefriends-header.csv")

# Select only age and numFriends columns, we don't need to show it, we just need to assign the results to a variable
friendsByAge = lines.select("age", "friends")

# From friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# Sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Formatted more nicely
# In order to aggregate or use special functions such as round and avg together you need to use a special function called .agg
# in other words, .agg allows us to clump together multiple commands on an aggregated group results
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
# we name the column name: friends_avg with the value from .alias('friends_avg) 
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()

spark.stop()
