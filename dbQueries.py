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
collection = db.aggregated_collection
# print(conn)
# print(db)
# print(collection)

# Get movies per release year (ascending) and vote avg (descending)


def getMoviesReleaseYear(topN=10):
    # Filter only movies in english
    movies = list(collection.find({"original_language": "en"}).sort()
                  [("release_date", 1), ("vote_average", -1)])
    movies_json = []
    # In case we get less results than the especified number
    topN = (len(movies) if len(movies) < topN else topN)
    # Iterate through collection to build the list
    for i in range(topN):
        movie_json = {
            "title": movie["original_title"],
            "genres": movie["genres"],
            "budget": movie["budget"],
            # New fields from aggregated collection
            "productionBudget": (0 if len(movie["aggregate"]) == 0 else round(movie["aggregate"][0]['productionBudget'], 3)),
            "domesticBudget": (0 if len(movie["aggregate"]) == 0 else round(movie["aggregate"][0]['domesticBudget'], 3)),
            "worldwideGross": (0 if len(movie["aggregate"]) == 0 else round(movie["aggregate"][0]['worldwideGross'], 3)),
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
# $unwind : Deconstructs an array field from the input here as genres then group and sort them base number of movies and revenue
# -1  decending / acending
# $group: Groups input documents by the specified _id expression and for each distinct grouping, outputs a document.
def getTopGenres(topN=10):
    movies = list(collection.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres",
                                                                            "avgRuntime": {"$avg": "$runtime"},
                                                                            "noMovies": {"$sum": 1},
                                                                            "avgPopularity": {"$avg": "$popularity"},
                                                                            "avgVotesNo": {"$avg": "$vote_count"},
                                                                            "avgProductionBudget": {"$avg": {"$arrayElemAt": ['$aggregate.productionBudget', 0]}},
                                                                            "avgDomesticBudget": {"$avg": {"$arrayElemAt": ['$aggregate.domesticBudget', 0]}},
                                                                            "avgWorldwideGross": {"$avg": {"$arrayElemAt": ['$aggregate.worldwideGross', 0]}},
                                                                            "avgBudget": {"$avg": "$budget"},
                                                                            "avgRevenue": {"$avg": "$revenue"},
                                                                            "avgVotes": {"$avg": "$vote_average"}}},
                                        {"$sort": {"noMovies": -1, "avgRevenue": -1}}]))
    movies_json = []
    # In case we get less results than the especified number
    topN = (len(movies) if len(movies) < topN else topN)
    for i in range(topN):
        movie_json = {
            "genres": movies[i]["_id"],
            "avgRuntime": round(movies[i]["avgRuntime"], 3),
            "noMovies": movies[i]["noMovies"],
            "avgPopularity": round(movies[i]["avgPopularity"], 3),
            "avgVotes": round(movies[i]["avgVotesNo"], 3),
            "avgBudget": round(movies[i]["avgBudget"], 3),
            # new fields from aggregation
            "avgProductionBudget": (0 if movies[i]["avgProductionBudget"] == None else round(movies[i]["avgProductionBudget"], 3)),
            "avgDomesticBudget": (0 if movies[i]["avgDomesticBudget"] == None else round(movies[i]["avgDomesticBudget"], 3)),
            "avgWorldwideGross": (0 if movies[i]["avgWorldwideGross"] == None else round(movies[i]["avgWorldwideGross"], 3)),
            "avgRevenue": round(movies[i]["avgRevenue"], 3),
            "avgVotes": round(movies[i]["avgVotes"], 3)
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 production companies
def getTopProductionCompanies(topN=10):
    movies = list(collection.aggregate([{"$unwind": "$production_companies"}, {"$group": {"_id": "$production_companies",
                                                                                          "avgRuntime": {"$avg": "$runtime"},
                                                                                          "noMovies": {"$sum": 1}, "avgPopularity": {"$avg": "$popularity"},
                                                                                          "avgVotesNo": {"$avg": "$vote_count"},
                                                                                          "avgProductionBudget": {"$avg": {"$arrayElemAt": ['$aggregate.productionBudget', 0]}},
                                                                                          "avgDomesticBudget": {"$avg": {"$arrayElemAt": ['$aggregate.domesticBudget', 0]}},
                                                                                          "avgWorldwideGross": {"$avg": {"$arrayElemAt": ['$aggregate.worldwideGross', 0]}},
                                                                                          "avgBudget": {"$avg": "$budget"},
                                                                                          "avgRevenue": {"$avg": "$revenue"},
                                                                                          "avgVotes": {"$avg": "$vote_average"}}},
                                        {"$sort": {"noMovies": -1, "avgRevenue": -1}}]))
    movies_json = []
    # In case we get less results than the especified number
    topN = (len(movies) if len(movies) < topN else topN)
    for i in range(topN):
        movie_json = {
            "production_companies": movies[i]["_id"],
            "noMovies": movies[i]["noMovies"],
            "avgPopularity": round(movies[i]["avgPopularity"], 3),
            "avgVotesNo": round(movies[i]["avgVotesNo"], 3),
            "avgBudget": round(movies[i]["avgBudget"], 3),
            # new fields from aggregation
            "avgProductionBudget": (0 if movies[i]["avgProductionBudget"] == None else round(movies[i]["avgProductionBudget"], 3)),
            "avgDomesticBudget": (0 if movies[i]["avgDomesticBudget"] == None else round(movies[i]["avgDomesticBudget"], 3)),
            "avgWorldwideGross": (0 if movies[i]["avgWorldwideGross"] == None else round(movies[i]["avgWorldwideGross"], 3)),
            "avgRevenue": round(movies[i]["avgRevenue"], 3),
            "avgVotes": round(movies[i]["avgVotes"], 3)
        }
        movies_json.append(movie_json)
    return movies_json

# Function to get top 10 revenue movies


def getTopMoreRevenue(topN=10):
    # Filter only movies in english
    movies = list(collection.find({"original_language": "en"}).sort(
        [("release_date", 1), ("revenue", -1)]))
    movies_json = []
    # In case we get less results than the especified number
    topN = (len(movies) if len(movies) < topN else topN)
    for i in range(topN):
        movie_json = {
            "title": movies[i]["original_title"],
            "genres": movies[i]["genres"],
            "budget": movies[i]["budget"],
            # New fields from aggregated collection
            "productionBudget": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['productionBudget'], 3)),
            "domesticBudget": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['domesticBudget'], 3)),
            "worldwideGross": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['worldwideGross'], 3)),
            "popularity": movies[i]["popularity"],
            "production_companies": movies[i]["production_companies"],
            "production_countries": movies[i]["production_countries"],
            "revenue": movies[i]["revenue"],
            "runtime": movies[i]["runtime"],
            "status": movies[i]["status"],
            "vote_avg": movies[i]["vote_average"],
            "vote_count": movies[i]["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json


# Function to get top 10 vote average
def getTopVoteAvg(topN=10):
    # Filter only movies in english
    movies = list(collection.find({"original_language": "en"}).sort(
        [("release_date", 1), ("vote_average", -1)]))
    movies_json = []
    # In case we get less results than the especified number
    topN = (len(movies) if len(movies) < topN else topN)
    for i in range(topN):
        movie_json = {
            "title": movies[i]["original_title"],
            "genres": movies[i]["genres"],
            "budget": movies[i]["budget"],
            # New fields from aggregated collection
            "productionBudget": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['productionBudget'], 3)),
            "domesticBudget": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['domesticBudget'], 3)),
            "worldwideGross": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['worldwideGross'], 3)),
            "popularity": movies[i]["popularity"],
            "production_companies": movies[i]["production_companies"],
            "production_countries": movies[i]["production_countries"],
            "revenue": movies[i]["revenue"],
            "runtime": movies[i]["runtime"],
            "status": movies[i]["status"],
            "vote_avg": movies[i]["vote_average"],
            "vote_count": movies[i]["vote_count"]
        }
        movies_json.append(movie_json)
    return movies_json


def wholeData(topN=1000):
    # Filter only movies in english
    movies = list(collection.find({"original_language": "en"}))
    movies_json = []
    # In case we get less results than the especified number
    topN = (len(movies) if len(movies) < topN else topN)
    for i in range(topN):
        movie_json = {
            "title": movies[i]["original_title"],
            "genres": movies[i]["genres"],
            "budget": movies[i]["budget"],
            "popularity": movies[i]["popularity"],
            "production_companies": movies[i]["production_companies"],
            "production_countries": movies[i]["production_countries"],
            "revenue": movies[i]["revenue"],
            # New fields from aggregated collection
            "productionBudget": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['productionBudget'], 3)),
            "domesticBudget": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['domesticBudget'], 3)),
            "worldwideGross": (0 if len(movies[i]["aggregate"]) == 0 else round(movies[i]["aggregate"][0]['worldwideGross'], 3)),
            "runtime": movies[i]["runtime"],
            "status": movies[i]["status"],
            "vote_avg": movies[i]["vote_average"],
            "vote_count": movies[i]["vote_count"],
            "release_date": movies[i]["release_date"]
        }
        movies_json.append(movie_json)
    return movies_json
