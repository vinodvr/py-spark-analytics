from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MovieRatingsDistribution")
sc = SparkContext(conf = conf)

movieId = 127 # movieID of Godfather is 127

linesRDD = sc.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")
movieRatingsRDD = linesRDD.filter(lambda x: int(x.split()[1]) == movieId)

ratingsHistogram = movieRatingsRDD \
                .map(lambda x: (int(x.split()[2]), 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()

print('Rating distribution is', ratingsHistogram)

