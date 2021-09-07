from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    # for every line in the file, split it by the pipe delimeter and assign the movieID int to the movie name, then return the dictionary movieNames
    with codecs.open("D:/scourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("ALSExample").getOrCreate()
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
names = loadMovieNames()
    
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("file:///scourse/ml-100k/u.data")
    
print("Training recommendation model...")

# ALS = Alternating Least Squares (matrix factorization), parameter of userID, ProductID and Rating
# An algorithm that essentially tries to find the right fitness level 
als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")
    
model = als.fit(ratings)

# IMPORTANTNote: since we are using argv it means that we need to enter in an argument when calling our script in the cli
# Manually construct a dataframe of the user ID's we want reccomendations for
# argv[1] gets the second parameter, argv[0] is the first, that was used when calling this script, 
userID = int(sys.argv[1])
# we then have to create a new schema based off the userID, because everything needs to be a dataframe now
userSchema = StructType([StructField("userID", IntegerType(), True)])
# [[userID,]] - is a special trick for spark to trick it into thinking that we are passing a column from a database
users = spark.createDataFrame([[userID,]], userSchema)

# use the function from our ALS algorithm .recommendForUserSubset to generate recommendations for our users
# the first parameter is the recommendation that we want to make for and the 2nd is to generate 10 recommendations and then collect that info
recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID " + str(userID))

for userRecs in recommendations:
    myRecs = userRecs[1]  #userRecs is (userID, [Row(movieId, rating), Row(movieID, rating)...])
    for rec in myRecs: #my Recs is just the column of recs for the user
        movie = rec[0] #For each rec in the list, extract the movie ID and rating
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))
        

