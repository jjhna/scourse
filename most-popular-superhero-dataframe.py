from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///scourse/Marvel-names.txt")

lines = spark.read.text("file:///scourse/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could throw off the counts.
# we simply grab the first item in the row, which will then be the superhero id that we are looking at
    # create a new column called connections that will count the size of all the superhero id's in that row, minus the 1st one
    # Now we need to count the sum of all the connections but also overwrite the .withColumn "connections" that we made earlier
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# .first() just gets and returns the most popular superhero
mostPopular = connections.sort(func.col("connections").desc()).first()

# we just need to flter & extract the string name from the most popular superhero
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

