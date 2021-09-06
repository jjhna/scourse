from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("test").getOrCreate()

df = StructType([ \
                     StructField("cust_id", IntegerType(), True), StructField("item_id", IntegerType(), True), StructField("price", FloatType(), True)])

dfa = spark.read.schema(df).csv("file:///scourse/customer-orders.csv")

something = dfa.groupBy("cust_id").agg(func.round(func.sum("price"), 2).alias("total_spent"))

results = something.sort("total_spent")

results.show(results.count())
    
spark.stop()

                                                  