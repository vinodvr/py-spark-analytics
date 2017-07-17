from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

# Create a SparkSession
spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

# Load up our data and convert it to the format MLLib expects.
ratingsRDD = spark.sparkContext.textFile("file:///Users/vinodvr/spark-python/ml-100k/u.data")

def daysSinceEpoch(timestamp):
    return int(timestamp/60/60/24)

ratingsPerDayDict = ratingsRDD.map(lambda x: x.split("\t")) \
                    .map(lambda x: daysSinceEpoch(int(x[3]))) \
                    .countByValue()

# prepare data frame as required by MLLib
data = spark.sparkContext.parallelize(ratingsPerDayDict.items()) \
        .map(lambda x: (float(x[1]), Vectors.dense(float(x[0]))))
df = data.toDF(["label", "features"])

# Let's split our data into training data and testing data
trainTest = df.randomSplit([0.5, 0.5])
trainingDF = trainTest[0]
testDF = trainTest[1]

# Now create the linear regression model
lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Train the model using our training data
model = lir.fit(trainingDF)

# Generate predictions for test data using our linear regression model 
fullPredictions = model.transform(testDF).cache()

# Extract the predictions and the "known" correct labels.
predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

# Zip them together
predictionAndLabel = predictions.zip(labels).collect()

# Print out the predicted and actual values for each point
for prediction in predictionAndLabel:
    print(prediction)

# Summarize the model over the training set and print out some metrics
trainingSummary = model.summary
print("r2: %f" % trainingSummary.r2)

# Stop the session
spark.stop()