import findspark

from training.training_helper import get_all_ratings_dict

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
spark = (SparkSession.builder
         .appName("ElasticsearchSparkMllibIntegration")
         .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,")
         .getOrCreate())

model = ALSModel.load("als_model")


def get_recommendation_from_movie(ratings_data, movie_id):
    model.transform(ratings_data)
    # Récupérer les recommandations Movie => Users
    recommended_users_for_movie = model.recommendForItemSubset(spark.createDataFrame([{'movie_id': movie_id}]), 10)
    print(recommended_users_for_movie.show())
    # TODO Transformer [recommended_users_for_movie] à un dataframe avec la liste des users
    # Récupérer les recommandations Users => Movies
    df_exploded = recommended_users_for_movie.select("recommendations", explode("recommendations").alias("user_id_col_with_rating"))
    df_user_ids_with_ratings = df_exploded.drop("recommendations")
    remove_rating = udf(lambda id_rating_tuple: id_rating_tuple[0], IntegerType())
    # users_ids = df_exploded.select("user_id_col", col("exploded_col").alias("user_id"))
    df_user_id = df_user_ids_with_ratings.withColumn("user_id", remove_rating("user_id_col_with_rating"))
    df_user_id = df_user_id.drop("user_id_col_with_rating")
    return model.recommendForUserSubset(df_user_id, 10)


ratings_dict = get_all_ratings_dict()
rating_df = spark.createDataFrame(ratings_dict)
res = get_recommendation_from_movie(rating_df, 390)
print(res)

print(res.show(100))