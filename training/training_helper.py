import requests

def get_all_ratings_dict():
    ratings_api_url = "http://localhost:5000/api/ratings"
    return requests.get(ratings_api_url).json()