import tmdbsimple as tmdb
from pykafka import KafkaClient
import json
# import os
import requests

tmdb.API_KEY = '77ac0a1314a3ce073d073a61984c1205'

#TODO need to replace these with environment variables
# tmdb.API_KEY = os.getenv('api-key)
# topic = os.getenv('topic)

# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['first_topic']

# using the producer to link to topic
producer = topic.get_sync_producer()

url_discover = f'https://api.themoviedb.org/3/discover/movie?api_key={tmdb.API_KEY}'
movie_list = requests.get(url_discover)
json_obj = movie_list.json()
results = json_obj['results']


# initializing a list to store the ids
ids = []
for obj in results:
    id = obj['id']
    # checking if there are duplicates in data
    if id not in ids:
        ids.append(id)


# ranging over all the movie ids to send data to kafka
movies = []
for id in ids:
    url_movie = f'https://api.themoviedb.org/3/movie/{id}?api_key={tmdb.API_KEY}'
    movie_data = requests.get(url_movie)
    json_obj = movie_data.json()
    movie = {}
    movie['id'] = id
    movie['title'] = json_obj['original_title']
    movie['popularity'] = json_obj['popularity']
    movie['revenue'] = json_obj['revenue']
    movie['budget'] = json_obj['budget']
    movie['genres'] = json_obj['genres']
    movie['release_date'] = json_obj['release_date']
    movie['runtime'] = json_obj['runtime']
    movies.append(movie)

# produce method requires data in bytes, so encoding it and sending
message = json.dumps(movies)
producer.produce(message.encode('ascii'))

# print(json_obj)