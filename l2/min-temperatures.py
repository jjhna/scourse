from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    # split each line using comments
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///scourse/1800.csv")
parsedLines = lines.map(parseLine)
# filter out all the entry type that contains a TMIN value
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# however since all the values are now related to TMIN we can continue to parse it even futher to just the stationID and temperature
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# reduce by key by skiming through all the stationID's and find the lowest recorded temmperature for that particular stationID
# find the lowest recorded temmperature for that particular stationID using the min function
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# then we just collect the results to make it look pretty
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
