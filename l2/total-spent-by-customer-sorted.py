from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

#Changed for Python 3 compatibility:
#flipped = totalByCustomer.map(lambda (x,y):(y,x))

# ImportantNOTE: In order to sort a value from the largest or smallest in a key, value pair
# you can only perform this action by sorting by the key not the value, so in this case we need to flip the key, value pair
# so that we can sort the flipped value by the key to get our results
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))

totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect();
for result in results:
    print(result)
