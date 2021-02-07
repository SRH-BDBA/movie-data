import config
import tmdbsimple as tmdb
import requests
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
import  pymongo

# Mongo DB connection
conn = config.MONGO_URL
client = pymongo.MongoClient(conn)
db = client["movies"]
collection = db.movies_collection

movies = []
# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['movies']



for msg in client.topics['movies'].get_simple_consumer():
    data = '{0}'.format(msg.value.decode())
    # Process should be listening all the time to receive new messages
    if msg is not None:
        data = '{0}'.format(msg.value.decode())
        # get data only in json format and ignore other data
        try:
            movie = json.loads(data)
            print('===== ',movie['original_title'])
            if len(list(collection.find({'_id':movie['_id']}))) == 0:
                collection.insert_one(movie)
                print('Movie validated and inserted')
            else:
                print('Movie validated and not inserted')
        except ValueError:
                    print('Another message that is not a movie')
                    print(data)
                    continue
        else:
                continue

    