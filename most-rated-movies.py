from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession 
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

# create a RDD from text file then transform it into a RDD with Row objects using map
linesRDD = spark.sparkContext.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")
ratingsRDD = linesRDD.map(mapper)

# Register the DataFrame as a table.
schemaRatings = spark.createDataFrame(ratingsRDD).cache()
schemaRatings.createOrReplaceTempView("movie_ratings")

# SQL can be run over DataFrames that have been registered as a table.
query = "SELECT movie_id, count(rating) as cnt FROM movie_ratings GROUP BY movie_id order by cnt desc limit 10"
top_rated = spark.sql(query)

# The results of SQL queries are RDDs and support all the normal RDD operations.
print('Most rated movies are:\n')
for top_movie in top_rated.collect():
  print(top_movie)
  
print('\n')
spark.stop()