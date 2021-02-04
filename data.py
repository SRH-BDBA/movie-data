import tmdbsimple as tmdb
from pykafka import KafkaClient
import json
# import os
import requests
import config




# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['test5']

# using the producer to link to topic
producer = topic.get_sync_producer()

url_discover = f'https://api.themoviedb.org/3/discover/movie?api_key={config.API_KEY}'
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
    url_movie = f'https://api.themoviedb.org/3/movie/{id}?api_key={config.API_KEY}&append_to_response=credits'
    movie_data = requests.get(url_movie)
    result = movie_data.json()
    movie = {}
    movie['_id'] = str(result['id'])
    movie['budget'] = result['budget']
    movie['genres'] = list(map(lambda m: m['name'], result['genres']))
    movie['original_language'] = result['original_language']
    movie['original_title'] = result['original_title']
    movie['popularity'] = result['popularity']
    movie['production_companies'] = list(map(lambda m: m['name'], result['production_companies']))
    movie['production_countries'] = list(map(lambda m: m['name'], result['production_countries']))
    movie['release_date'] = result['release_date']
    movie['revenue'] = result['revenue']
    movie['runtime'] = result['runtime']
    movie['status'] = result['status']
    movie['vote_average'] = result['vote_average']
    movie['vote_count'] = result['vote_count']
    movie['cast'] = result['credits']['cast']
    movie['crew'] = result['credits']['crew']
    movies.append(movie)

# produce method requires data in bytes, so encoding it and sending
message = json.dumps(movies)
producer.produce(message.encode('ascii'))
print("Count",len(message))

# print(json_obj)