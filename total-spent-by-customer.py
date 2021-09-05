from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
# https://youtu.be/RE_L6silAJE?t=145
# In this specific example the reducebykey function takes a lambda function 
# It takes 2 arguments, which represents two values that have the SAME key
# so we calculate the sum of the two values and return it to a single value to the SAME key
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect();
for result in results:
    print(result)
