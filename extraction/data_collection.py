from flask import Flask, jsonify
import pandas as pd

app = Flask(__name__)

# Read MovieLens files
users_cols = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
users = pd.read_csv('../data/u.user', sep='|', names=users_cols)

ratings_cols = ['user_id', 'movie_id', 'rating', 'unix_timestamp']
ratings = pd.read_csv('../data/u.data', sep='\t', names=ratings_cols, encoding='latin-1')

genre_cols = [
    "genre_unknown", "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]

movies_cols = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + genre_cols
movies = pd.read_csv('../data/u.item', sep='|', names=movies_cols, encoding='latin-1')

# API to get information about users
@app.route('/api/users', methods=['GET'])
def get_users():
    return jsonify(users.to_dict(orient='records'))

# API to get user ratings
@app.route('/api/ratings', methods=['GET'])
def get_ratings():
    return jsonify(ratings.to_dict(orient='records'))

# API to get information about movies
@app.route('/api/movies', methods=['GET'])
def get_movies():
    return jsonify(movies.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)