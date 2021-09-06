from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///scourse/Marvel-names.txt")

lines = spark.read.text("file:///scourse/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim whitespace from each line as this
# could throw the counts off by one.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# we have [0] because we just need to grab the first value in each row from the connectiosn column
# we also use min to grab the smallest amount of connections in the list
minConnectionCount = connections.agg(func.min("connections")).first()[0]

# now we need to filter it so that we get the connections that are equal to 1
minConnections = connections.filter(func.col("connections") == minConnectionCount)

# now we join the original data frame from our schema and our minConnection based off the id
minConnectionsWithNames = minConnections.join(names, "id")

print("The following characters have only " + str(minConnectionCount) + " connection(s):")

# we then use sqls select to show the first 20 results of the name column 
minConnectionsWithNames.select("name").show()