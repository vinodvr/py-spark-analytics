from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MovieRatingsDistribution")
sc = SparkContext(conf = conf)

movieId = '127'   # movieID of Godfather is 127
movieIdColIdx = 1 # 2nd column has movieId in data file
ratingColIdx = 2  # 3rd column has the actual rating given by a user
 
linesRDD = sc.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")
movieRatingsRDD = linesRDD.filter(lambda x: x.split()[movieIdColIdx] == movieId)

ratingsHistogram = movieRatingsRDD \
                .map(lambda x: (x.split()[ratingColIdx], 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()

print('Rating distribution is', ratingsHistogram)

