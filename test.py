from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

def parseLine(line):
    # split each line using comments
    fields = line.split(',')
    customerID = int(fields[0])
    price = float(fields[2])
    return (customerID, price)

lines = sc.textFile("file:///scourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
total = parsedLines.reduceByKey(lambda x, y: x + y)

flipped = total.map(lambda x: (x[1], x[0]))
realtotal = flipped.sortByKey()

results = realtotal.collect()

for result in results:
    print(result)
