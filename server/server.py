from re import template

import pandas as pd
from flask import Flask, jsonify, request, url_for, redirect, render_template

from training.als_recommendation import init_spark, load_model, get_recommendations_for_movie_with_score

# forms.py

from wtforms import Form, StringField, SelectField

app = Flask(__name__)

# Read MovieLens files
users_cols = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
users = pd.read_csv('../data/u.user', sep='|', names=users_cols)

ratings_cols = ['user_id', 'movie_id', 'rating', 'unix_timestamp']
ratings = pd.read_csv('../data/u.data', sep='\t', names=ratings_cols, encoding='latin-1')
ratings_spark_df = None

genre_cols = [
    "genre_unknown", "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]

movies_cols = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + genre_cols
movies = pd.read_csv('../data/u.item', sep='|', names=movies_cols, encoding='latin-1')

# Create a new column 'genres' containing a list of genres
movies['genres'] = movies.apply(lambda row: [col for col in genre_cols if row[col] == 1], axis=1)

# Drop the individual genre columns
movies = movies.drop(columns=genre_cols)

# Join ratings and  movies on movie_id
combined_df = pd.merge(ratings, movies, on='movie_id')

# Join the combined_df with users based on user_id
final_df = pd.merge(combined_df, users, on='user_id')


# API to get information about users
@app.route('/api/users', methods=['GET'])
def get_users():
    return jsonify(users.to_dict(orient='records'))


# API to get user ratings
@app.route('/api/ratings', methods=['GET'])
def get_ratings():
    # Pagination example: ?limit=10&offset=0
    limit = request.args.get('limit', default=10, type=int)
    offset = request.args.get('offset', default=None, type=int)
    df_results = ratings if offset is None else ratings.loc[offset:offset + limit - 1]
    return jsonify(df_results.to_dict(orient='records'))


# API to get information about movies
@app.route('/api/movies', methods=['GET'])
def get_movies():
    return jsonify(movies.to_dict(orient='records'))


# API to get information about dataset combined collection
@app.route('/api/combined_collection', methods=['GET'])
def get_combined_collection():
    return jsonify(final_df.to_dict(orient='records'))


@app.route('/api/recommendation/<int:movie_id>', methods=['GET'])
def get_movie_recommendations(movie_id):
    recommended_movies_ids_with_scores = get_recommendations_for_movie_with_score(movie_id, ratings=ratings_spark_df, spark_session=spark,
                                                                      als_model=model)
    recommended_movies_ids = [x[0] for x in recommended_movies_ids_with_scores]
    recommended_movies = movies.loc[movies['movie_id'].isin(recommended_movies_ids)]
    return recommended_movies.to_json(orient='records')


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    spark = init_spark()
    ALS_MODEL_PATH = "../training/als_model"
    ratings.iteritems = ratings.items
    ratings_spark_df = spark.createDataFrame(ratings)
    model = load_model(ALS_MODEL_PATH, df_rattings=ratings_spark_df)
    app.run(debug=True)
