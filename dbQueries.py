from collections import OrderedDict
import math
from datetime import datetime
import pymongo
import config
import pandas as pd

# Mongo connection
conn = config.MONGO_URL
client = pymongo.MongoClient(conn)
db = client["movies"]
collection = db.movies_collection
# print(conn)
# print(db)
# print(collection)


# Get movies per release year (ascending) and vote avg (descending)
def getMoviesReleaseYear():
    movies = collection.find().sort(
        [("release_date", 1), ("vote_average", -1)])
    movies_json = []
    # Iterate through collection to build the list
    for movie in movies:
        movie_json = {
            "title": movie["original_title"],
            "genres": movie["genres"],
            "budget": movie["budget"],
            "popularity": movie["popularity"],
            "production_companies": movie["production_companies"],
            "production_countries": movie["production_countries"],
            "revenue": movie["revenue"],
            "runtime": movie["runtime"],
            "status": movie["status"],
            "vote_avg": movie["vote_average"],
            "vote_count": movie["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 genres
def getTopGenres(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = list(collection.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "avgRuntime": {"$avg": "$runtime"}, "noMovies": {"$sum": 1}, "avgPopularity": {"$avg": "$popularity"}, "avgVotesNo": {
                  "$avg": "$vote_count"}, "avgBudget": {"$avg": "$budget"}, "avgRevenue": {"$avg": "$revenue"}, "avgVotes": {"$avg": "$vote_average"}}}, {"$sort": {"noMovies": -1, "avgRevenue": -1}}]))
    movies_json = []
    for i in range(topN):
        movie_json = {
            "genres": movies[i]["_id"],
            "avgRuntime": round(movies[i]["avgRuntime"], 3),
            "noMovies": movies[i]["noMovies"],
            "avgPopularity": round(movies[i]["avgPopularity"], 3),
            "avgVotes": round(movies[i]["avgVotesNo"], 3),
            "avgBudget": round(movies[i]["avgBudget"], 3),
            "avgRevenue": round(movies[i]["avgRevenue"], 3),
            "avgVotes": round(movies[i]["avgVotes"], 3)
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 production companies
def getTopProductionCompanies(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = list(collection.aggregate([{"$unwind": "$production_companies"}, {"$group": {"_id": "$production_companies", "avgRuntime": {"$avg": "$runtime"}, "noMovies": {"$sum": 1}, "avgPopularity": {
                  "$avg": "$popularity"}, "avgVotesNo": {"$avg": "$vote_count"}, "avgBudget": {"$avg": "$budget"}, "avgRevenue": {"$avg": "$revenue"}, "avgVotes": {"$avg": "$vote_average"}}}, {"$sort": {"noMovies": -1, "avgRevenue": -1}}]))
    movies_json = []
    for i in range(topN):
        movie_json = {
            "production_companies": movies[i]["_id"],
            "noMovies": movies[i]["noMovies"],
            "avgPopularity": round(movies[i]["avgPopularity"], 3),
            "avgVotesNo": round(movies[i]["avgVotesNo"], 3),
            "avgBudget": round(movies[i]["avgBudget"], 3),
            "avgRevenue": round(movies[i]["avgRevenue"], 3),
            "avgVotes": round(movies[i]["avgVotes"], 3)
        }
        movies_json.append(movie_json)
    return movies_json

# Function to get top 10 revenue movies


def getTopMoreRevenue(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = collection.find().sort([("release_date", 1), ("revenue", -1)])
    movies_json = []
    for movie in movies:
        movie_json = {
            "title": movie["original_title"],
            "genres": movie["genres"],
            "budget": movie["budget"],
            "popularity": movie["popularity"],
            "production_companies": movie["production_companies"],
            "production_countries": movie["production_countries"],
            "revenue": movie["revenue"],
            "runtime": movie["runtime"],
            "status": movie["status"],
            "vote_avg": movie["vote_average"],
            "vote_count": movie["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 vote average
def getTopVoteAvg(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = collection.find().sort(
        [("release_date", 1), ("vote_average", -1)])
    movies_json = []
    for movie in movies:
        movie_json = {
            "title": movie["original_title"],
            "genres": movie["genres"],
            "budget": movie["budget"],
            "popularity": movie["popularity"],
            "production_companies": movie["production_companies"],
            "production_countries": movie["production_countries"],
            "revenue": movie["revenue"],
            "runtime": movie["runtime"],
            "status": movie["status"],
            "vote_avg": movie["vote_average"],
            "vote_count": movie["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json


def wholeData(topN=10):
    movies = collection.find()
    movies_json = []
    for movie in movies:
        movie_json = {
            "title": movie["original_title"],
            "genres": movie["genres"],
            "budget": movie["budget"],
            "popularity": movie["popularity"],
            "production_companies": movie["production_companies"],
            "production_countries": movie["production_countries"],
            "revenue": movie["revenue"],
            "runtime": movie["runtime"],
            "status": movie["status"],
            "vote_avg": movie["vote_average"],
            "vote_count": movie["vote_count"],
            "release_date": movie["release_date"]
        }
        movies_json.append(movie_json)
    return movies_json
