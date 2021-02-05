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
print(conn)
print(db)
print(collection)


# Get movies per release year (ascending) and vote avg (descending)
def getMoviesReleaseYear():
    print(f" Movies in English: {collection.count(({'original_language':'en'}))}")
    movies = collection.find().sort([("release_date",1),("vote_average",-1)])
    movies_json = []
    # Iterate through collection to build the list
    for movie in movies:
        movie_json = {
            "title" : movie["original_title"],
            "genres" : movie["genres"],
            "budget" : movie["budget"],
            "popularity" : movie["popularity"],
            "production_companies" : movie["production_companies"],
            "production_countries" : movie["production_countries"],
            "revenue" : movie["revenue"],
            "runtime" : movie["runtime"],
            "status" : movie["status"],
            "vote_avg" : movie["vote_average"],
            "vote_count" : movie["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json



# Function to get top 10 genres
def getTopGenres(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = list(collection.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","avgRuntime":{"$avg":"$runtime"},"noMovies":{"$sum":1},"avgPopularity":{"$avg":"$popularity"},"avgVotesNo":{"$avg":"$vote_count"},"avgBudget":{"$avg":"$budget"},"avgRevenue":{"$avg":"$revenue"},"avgVotes":{"$avg":"$vote_average"}}},{"$sort": {"noMovies":-1,"avgRevenue":-1}}]))
    # -- Use aggregate to group fields and calculate aritmethic functions like SQL
    # collection.aggregate([
    #   -- Genres is an collection of documents. We need to group them separetely with the unwind operator
    #   {"$unwind":"$genres"},
    #   -- This is where the grouping occurs
    #   {"$group":
    #           {"_id":"$genres",                           -- The id of the grouping should be each genre
    #           "avgRuntime":{"$avg":"$runtime"},           -- Calculate avg of runtime and name it 'avgRunTime'
    #           "noMovies":{"$sum":1},                      -- Count the movies (like sum((1) in sql for each movie)
    #           "avgPopularity":{"$avg":"$popularity"},     -- Calculate the avg of popularity and name it 'avgPopularity'
    #           "avgVotesNo":{"$avg":"$vote_count"},        -- Calculate the avg of vote_count and name it 'avgVotesNo'
    #           "avgBudget":{"$avg":"$budget"},             -- Calculate the avg of budget and name it 'avgBudget'
    #           "avgRevenue":{"$avg":"$revenue"},           -- Calculate the avg of revenue and name it 'avgRevenue'
    #           "avgVotes":{"$avg":"$vote_average"}}},      -- Calculate the avg of vote_average and name it 'avgVotes'
    #   -- Sort the results with two criteria
    #   {"$sort": 
    #           {"noMovies":-1,     -- by number of movies descending (-1) | ascending should be 1
    #           "avgRevenue":-1}    -- by avgRevenue descending (-1)
    #   }
    # ]))
    movies_json = []
    for i in range(topN):
        movie_json = {
            "genres" : movies[i]["_id"],
            "avgRuntime" : round(movies[i]["avgRuntime"],3),
            "noMovies" : movies[i]["noMovies"],
            "avgPopularity" : round(movies[i]["avgPopularity"],3),
            "avgVotes" : round(movies[i]["avgVotesNo"],3),
            "avgBudget" : round(movies[i]["avgBudget"],3),
            "avgRevenue" : round(movies[i]["avgRevenue"],3),
            "avgVotes" : round(movies[i]["avgVotes"],3)
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 production companies
def getTopProductionCompanies(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = list(collection.aggregate([{"$unwind":"$production_companies"},{"$group":{"_id":"$production_companies","avgRuntime":{"$avg":"$runtime"},"noMovies":{"$sum":1},"avgPopularity":{"$avg":"$popularity"},"avgVotesNo":{"$avg":"$vote_count"},"avgBudget":{"$avg":"$budget"},"avgRevenue":{"$avg":"$revenue"},"avgVotes":{"$avg":"$vote_average"}}},{"$sort": {"noMovies":-1,"avgRevenue":-1}}]))
    movies_json = []
    for i in range(topN):
        movie_json = {
            "production_companies" : movies[i]["_id"],
            "noMovies" : movies[i]["noMovies"],
            "avgPopularity" : round(movies[i]["avgPopularity"],3),
            "avgVotesNo" : round(movies[i]["avgVotesNo"],3),
            "avgBudget" : round(movies[i]["avgBudget"],3),
            "avgRevenue" : round(movies[i]["avgRevenue"],3),
            "avgVotes" : round(movies[i]["avgVotes"],3)
        }
        movies_json.append(movie_json)
    return movies_json

# Function to get top 10 revenue movies
def getTopMoreRevenue(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = collection.find().sort([("release_date",1),("revenue",-1)])
    movies_json = []
    for movie in movies:
        movie_json = {
            "title" : movie["original_title"],
            "genres" : movie["genres"],
            "budget" : movie["budget"],
            "popularity" : movie["popularity"],
            "production_companies" : movie["production_companies"],
            "production_countries" : movie["production_countries"],
            "revenue" : movie["revenue"],
            "runtime" : movie["runtime"],
            "status" : movie["status"],
            "vote_avg" : movie["vote_average"],
            "vote_count" : movie["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 vote average
def getTopVoteAvg(topN=10):
    # Get the authors who wrote the highest number of books and got the highest rating
    movies = collection.find().sort([("release_date",1),("vote_average",-1)])
    movies_json = []
    for movie in movies:
        movie_json = {
            "title" : movie["original_title"],
            "genres" : movie["genres"],
            "budget" : movie["budget"],
            "popularity" : movie["popularity"],
            "production_companies" : movie["production_companies"],
            "production_countries" : movie["production_countries"],
            "revenue" : movie["revenue"],
            "runtime" : movie["runtime"],
            "status" : movie["status"],
            "vote_avg" : movie["vote_average"],
            "vote_count" : movie["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json

