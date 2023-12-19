import findspark

from training.training_helper import get_all_ratings_dict

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType


def calculate_score(users_recommendations):
    """
    score_movie_id = somme(1/k, n) avec k l'ordre de la recommandation et n le nombre d'occurence de movie_id
    """
    # Score dictionnary with MOVIE_ID as key and its score as a value
    scores_dictionnary = {}
    for user_recommendations in users_recommendations:
        for index, recommendation in enumerate(user_recommendations):
            if recommendation in scores_dictionnary:
                scores_dictionnary[recommendation] += 1 / (index + 1)
            else:
                scores_dictionnary[recommendation] = 1 / (index + 1)

    return scores_dictionnary


def get_recommendation_from_movie(als_model, spark_session, ratings_data, movie_id):
    # Récupérer les recommandations Movie => Users
    recommended_users_for_movie = als_model.recommendForItemSubset(
        spark_session.createDataFrame([{'movie_id': movie_id}]), 5)
    print(recommended_users_for_movie.show())
    # TODO Transformer [recommended_users_for_movie] à un dataframe avec la liste des users
    # Récupérer les recommandations Users => Movies
    df_exploded = recommended_users_for_movie.select("recommendations",
                                                     explode("recommendations").alias("user_id_col_with_rating"))
    df_user_ids_with_ratings = df_exploded.drop("recommendations")
    remove_rating = udf(lambda id_rating_tuple: id_rating_tuple[0], IntegerType())
    # users_ids = df_exploded.select("user_id_col", col("exploded_col").alias("user_id"))
    df_user_id = df_user_ids_with_ratings.withColumn("user_id", remove_rating("user_id_col_with_rating"))
    df_user_id = df_user_id.drop("user_id_col_with_rating")

    # Get top 5 users recommendations
    return als_model.recommendForUserSubset(df_user_id, 5)


# Define a UDF to extract the first element of each tuple
def extract_first_element(lst):
    return [tup[0] for tup in lst]


def get_recommended_movies(df_users_recommendations, movie_id):
    # Clean dataframe
    # Register the UDF
    extract_first_element_udf = udf(extract_first_element, ArrayType(IntegerType()))
    df_users_recommendations_cleaned = df_users_recommendations.withColumn("recommendations", extract_first_element_udf(
        df_users_recommendations["recommendations"]))
    print(df_users_recommendations_cleaned.show(10, truncate=False))
    # score_movie_id = somme(1/k, n) avec k l'ordre de la recommandation et n le nombre d'occurence de movie_id
    # Extract recommendations column from dataframe into a list
    list_users_recommendations = df_users_recommendations_cleaned.select("recommendations").rdd.flatMap(
        lambda x: x).collect()
    movies_scores = calculate_score(list_users_recommendations)
    print(f"movies_scores: {movies_scores}")

    # Remove the id of the searched movie
    # Delete movie_id if it exists within the recommended movies
    if movie_id in movies_scores:
        del movies_scores[movie_id]

    sorted_movies_scores = sorted(movies_scores.items(), key=lambda x: x[1], reverse=True)
    print(f"sorted_movies_scores: {sorted_movies_scores}")

    return sorted_movies_scores


def init_spark():
    return (SparkSession.builder
            .appName("ElasticsearchSparkMllibIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,")
            .getOrCreate())


def load_model(model_name, df_rattings):
    loaded_model = ALSModel.load(model_name)
    loaded_model.transform(df_rattings)
    return loaded_model


def get_df_ratings(spark_session):
    # TODO Il faudra passer par ES au lieu d'appeler l'endpoint des "ratings"
    ratings_dict = get_all_ratings_dict()
    return spark_session.createDataFrame(ratings_dict)


def get_recommendations_for_movie_with_score(movie_id: int, ratings, spark_session, als_model):
    """Main function that returns the recommended movies from movie search"""
    _df_users_recommendations = get_recommendation_from_movie(als_model, spark_session, ratings, movie_id)
    print(_df_users_recommendations)
    print(_df_users_recommendations.show(10, truncate=False))
    recommended_movies = get_recommended_movies(_df_users_recommendations, movie_id=movie_id)
    print(f'recommended movies {recommended_movies}')
    return recommended_movies


if __name__ == '__main__':
    # This is an example of how to get recommendations for a movie search with a certain ID
    spark = init_spark()
    df_ratings = get_df_ratings(spark_session=spark)
    model = load_model("als_model", df_rattings=df_ratings)
    get_recommendations_for_movie_with_score(390, df_ratings, spark_session=spark, als_model=model)