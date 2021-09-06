from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# the first option asks if we have a header row in our csv file which is true
# the second option asks if we have a inferShcema in our csv file
# spark will automatically go through the csv file and infer the schemea of each column. 
# IN OTHER WORDS it will automatically guess the data types of each field if set to true
# note this is only useful if there are headers for each column to classify with
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///scourse/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()

