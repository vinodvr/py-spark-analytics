from pyspark import SparkConf, SparkContext

# create SparkContext using standalone mode
conf = SparkConf().setMaster("local").setAppName("MovieRatingsDistribution")
sc = SparkContext(conf = conf)

movieId = '127'   # movieID of Godfather is 127
movieIdColIdx = 1 # 2nd column has movieId in data file
ratingColIdx = 2  # 3rd column has the actual rating given by a user
 
 # create the RDD and filter it to only include desired movie ratings
linesRDD = sc.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")
movieRatingsRDD = linesRDD.filter(lambda x: x.split()[movieIdColIdx] == movieId)

# run a mapper and reducer function to calculate rating distribution
ratingsHistogram = movieRatingsRDD \
                .map(lambda x: (x.split()[ratingColIdx], 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()

print('\nRating distribution is for movie Godfather', ratingsHistogram)
print('\n')

