from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# ImportantNOTE: the forward slash \ allows the user to use row spaces to make code neater, otherwise the code will break
# Create schema when reading customer-orders
customerOrderSchema = StructType([ \
                                  StructField("cust_id", IntegerType(), True),
                                  StructField("item_id", IntegerType(), True),
                                  StructField("amount_spent", FloatType(), True)
                                  ])

# Load up the data into spark dataset
customersDF = spark.read.schema(customerOrderSchema).csv("file:///scourse/customer-orders.csv")

# ImportantNOTE: note that the agg - aggregate function includes the alias function as well (its just in a sepearate line)
# this ensures that both rounding, summing and alias functions are all called at the same time
# first we add up or sum all the amount spent for each unique customer id AND we round the results by 2 decimal points (the 2 in the 2nd argument)
totalByCustomer = customersDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2) \
                                      .alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
