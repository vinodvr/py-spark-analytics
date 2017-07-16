from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

linesRDD = sc.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")
ratingsRDD = linesRDD.map(lambda x: x.split()[2])
result = ratingsRDD.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
print('Rating Count')
for key, value in sortedResults.items():
    print("%s %i" % (key, value))