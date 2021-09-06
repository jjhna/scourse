from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
# the "sep" means sepearator and the "\t" means by tab, so we are separating the data file by tabs
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///scourse/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
# so we group by the movie ID 
# and then create a new column called count and count how many are times the movieID shows up in our dataset
# then we order that same new column called "count" and order it from largest to smallest
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
