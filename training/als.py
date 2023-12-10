# Import necessary libraries
import findspark
findspark.init()

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row, SparkSession
import requests

ALS_MODEL_PATH = "als_model"

def get_all_ratings_dict():
    ratings_api_url = "http://localhost:5000/api/ratings"
    return requests.get(ratings_api_url).json()


ratings_dict = get_all_ratings_dict()

spark = (SparkSession.builder
         .appName("ElasticsearchSparkMllibIntegration")
         .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,")
         .getOrCreate())

# lines = spark.read.text("data/mllib/als/sample_movielens_ratings.txt").rdd
# parts = lines.map(lambda row: row.value.split("::"))
# ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
#                                      rating=float(p[2]), timestamp=int(p[3])))

ratings = spark.createDataFrame(ratings_dict)
(training, test) = ratings.randomSplit([0.9, 0.1])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating",
          coldStartStrategy="drop")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

model.save(ALS_MODEL_PATH)

