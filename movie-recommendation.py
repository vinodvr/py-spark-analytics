import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

# create a Spark context
conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationSystem")
sc = SparkContext(conf = conf)
sc.setCheckpointDir('checkpoint')

# from the movies catalog file create a dictionary of movieID to name
def loadMovieNamesDict():
    movieNames = {}
    with open("ml-100k/u.ITEM", encoding='ascii', errors="ignore") as file:
        for line in file:
            fields = line.split('|')
            movieID = int(fields[0])
            movieName = fields[1]
            movieNames[movieID] = movieName
    return movieNames

print("Loading movie names dictionary..")
movieNamesDict = loadMovieNamesDict()

# now lets get to the ratings data
# lets create a RDD with Rating objects as expected by MLLib
ratings = sc.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")
ratingsRDD = ratings.map(lambda l: l.split('\t')) \
            .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))) \
            .cache()

# Build the recommendation model using Alternating Least Squares
print("Training recommendation model...")
rank = 10
numIterations = 6
model = ALS.train(ratingsRDD, rank, numIterations)

# the user for which we need to recommend
userID = int(sys.argv[1])

# lets print the ratings given by this user..
print("\nRatings given by userID " + str(userID) + ":")
userRatings = ratingsRDD.filter(lambda l: l[0] == userID)
for rating in userRatings.collect():
    print (movieNamesDict[int(rating[1])] + ": " + str(rating[2]))

# now lets use our model to recommend movies for this user..
print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print (movieNamesDict[int(recommendation[1])] + \
        " score " + str(recommendation[2]))
        
print('\n')