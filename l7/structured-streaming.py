# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
# So basically spark stream is sending the logs of text data into the file called logs which will then be used for 
accessLines = spark.readStream.text("logs")

# Parse out the common log format to a DataFrame
# so we need to convert our data: 66.249.75.159 - - [29/Nov/2015:03:50:05 +0000] "GET /robots.txt HTTP/1.1" 200 55 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
# and convert it into a dataframe 
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

# then we need to assign each data column into its own special variable such as the first text being the host
# and then the next being the timestamp: [29/Nov/2015:03:50:05 +0000]
logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count of every access by status code
statusCountsDF = logsDF.groupBy(logsDF.status).count()

# Kick off our streaming query, dumping results to the console
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

