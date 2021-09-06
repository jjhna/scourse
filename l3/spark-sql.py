from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# ImportantNOTE: the forward slash \ allows the user to use row spaces to make code neater, otherwise the code will break

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
# first we need to create a data frame from our variable people and keep its cache in memory for the next line
schemaPeople = spark.createDataFrame(people).cache()
# then we need to create or replace a temp view into the SQL world with a SQL database called "people"
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
# then we call a SQL command using the people table, however keep in mind that we still need to follow the variable rules that we 
# created in the define mapper section, such as age = int, etc. 
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
# in other words we can perform SQL actions using functions. 
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
