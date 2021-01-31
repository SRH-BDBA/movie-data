import tmdbsimple as tmdb
import requests
# import pandas as pd
# from pymongo import MongoClient
# from kafka import KafkaProducer
# import subprocess
from pykafka import KafkaClient

import json

tmdb.API_KEY = '77ac0a1314a3ce073d073a61984c1205'

# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['movie']

# using the producer to link to topic
producer = topic.get_sync_producer()

# # Mongo DB connection
# url = f'mongodb://localhost:27017/movies'
# conn = MongoClient(url)
# db = conn["movies"]
# collection = db.movies_collection

# # Test connection and get results
# movies_db = collection.find()
# movies_l=list(movies_db)
# print(len(movies_l))
# for movie_db in movies_l:
#    print(movie_db)
#    break

# URL for Genres
url_genre = f'https://api.themoviedb.org/3/genre/movie/list?api_key={tmdb.API_KEY}&language=en-US'
print(url_genre)
genres = requests.get(url_genre)
json_obj = genres.json()
res = json_obj['genres']

genres = []
# Iterate over the results
for g in res:
    genre = {}
    genre['id'] = g['id']
    genre['name'] = g['name']
    genres.append(genre)

url_search = f'https://api.themoviedb.org/3/discover/movie?primary_release_year=2020&sort_by=vote_average.desc&api_key={tmdb.API_KEY}&vote_count.gte=100'

print(url_search)
res = requests.get(url_search)
json_obj = res.json()
results = json_obj['results']

# Get paginated results from the API
page = int(json_obj['page'])
total_pages = int(json_obj['total_pages'])
i = 1
for i in range(page, 4):
    movies = []
    duplicates = []
    # Call API starting from second page
    if (i > 1):
        url_search = f'https://api.themoviedb.org/3/discover/movie?primary_release_year=2020&  sort_by=vote_average.desc&api_key={tmdb.API_KEY}&vote_count.gte=100'
        # Add page to get results
        url_search += f'&page={i}'
        print(f'{i} page')
        res = requests.get(url_search)
        json_obj = res.json()
        results = json_obj['results']

        # send data to kafka
       # produce method requires data in bytes, so encoding it and sending
        message = json.dumps(results)
        producer.produce(message.encode('ascii'))
    else:
        print('First page')
    # Verify results and build documents
    for result in results:
        movie = {}
        movie['_id'] = result['id']
        movie['original_language'] = result['original_language']
        movie['original_title'] = result['original_title']
        movie['popularity'] = result['popularity']
        movie['release_date'] = result['release_date']
        movie['vote_average'] = result['vote_average']
        movie['vote_count'] = result['vote_count']
        movie['genre_ids'] = result['genre_ids']

        # # Replace genre ids by dedscriptions
        # try:
        #     parse_res = []
        #     # Iterate on the ids results
        #     for r in result['genre_ids']:
        #         # Look for the equivalent description
        #         for gr in genres:
        #             if (gr['id'] == r):
        #                 parse_res.append(gr['name'])
        #                 break
        # #     # Add genre descriptions to document
        # #     movie['genre_ids'] = parse_res
        # # except:
        # #     movie['genre_ids'] = []
        # # finally:
        # #     # Validate if document already exists
        # #     if len(list(collection.find({'_id': movie['_id']}))) == 0:
        # #         movies.append(movie)
        # #     else:
        # #         duplicates.append(movie['_id'])

    # # Add results to Mongo
    # try:
    #     print(f'Len: {len(movies)}')
    #     collection.insert_many(movies)
    #     print(f'Collection {i} added ({len(movies)})')
    # except TypeError:
    #     print('Not results to add')

# Delete all documents in collection
# collection.delete_many({})
