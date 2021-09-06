from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///scourse/Marvel-names.txt")

lines = spark.read.text("file:///scourse/Marvel-graph.txt")


connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))


minCount = connections.agg(func.min("connections")).first()[0]

minConnect = connections.filter(func.col("connections") == minCount)

#mostPopular.show(10, False)

minConnectNames = minConnect.join(names, "id")

minConnectNames.select("name", "connections").show()

#print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

