import config
import tmdbsimple as tmdb
import requests
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
import pymongo

# Mongo DB connection
conn = config.MONGO_URL
client = pymongo.MongoClient(conn)
db = client["movies"]
collection = db.movies_collection

movies = []
# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['test5']

# # try:
# consumer = topic.get_simple_consumer(
#     consumer_group='test-consumer-group',
#     auto_offset_reset=OffsetType.LATEST)
# Process should be listening all the time to receive new messages
for i in topic.get_simple_consumer():
     data = '{0}'.format(i.value.decode())
     # get data only in json format and ignore other data
     try:
        movies = json.loads(data)
        for movie in movies:
            print('===== ', movie['original_title'])
            if len(list(collection.find({'_id': movie['_id']}))) == 0:
                collection.insert_one(movie)
                print('Movie validated and inserted')
            else:
                print('Movie validated and not inserted')
     except ValueError:
        print('Another message that is not a movie')
        print(data)
        continue

