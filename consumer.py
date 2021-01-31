import config
import tmdbsimple as tmdb
import requests
import json
from pykafka import KafkaClient
from pymongo import MongoClient

# Mongo DB connection
conn = config.MONGO_URL
client = pymongo.MongoClient(conn)
db = client["movies"]
collection = db.movies_collection

movies = []
# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
try:
    for i in client.topics['movies'].get_simple_consumer():
        data = '{0}'.format(i.value.decode())
        # get data only in json format and ignore other data
        try:
            movie = json.loads(data)
            movies.append(movie)
            print('Movie appended')
        except ValueError:
            continue
        
except KeyboardInterrupt:
    print('Process stopped')
    # Validate if movie already exists
    insert_movies = []
    print(config.Config.MONGO_URL)
    for movie in movies:
        if len(list(collection.find({'_id':movie['_id']}))) == 0:
            insert_movies.append(movie)
    # Add results to Mongo
    try:
        # Print how many documents it already has
        print(f'Len: {len(movies)}')
        collection.insert_many(insert_movies)
        print(f'Added ({len(movies)})')
    except TypeError:
        print('Not results to add')
    




