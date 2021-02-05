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

try:
    consumer = topic.get_simple_consumer(
        consumer_group='test-consumer-group', 
        auto_offset_reset=OffsetType.LATEST)
    # Process should be listening all the time to receive new messages
    while True:
        # Validate initial
        if len(movies) > 0:
                collection.insert_many(movies)
                print(f'{len(movies)} records successfully inserted')
                movies = []
                continue

        for i in consumer:
            if i is not None:
                if len(movies) == 50:
                    # Insert multiple records and 
                    collection.insert_many(movies)
                    print(f'{len(movies)} records successfully inserted')
                    movies = []

                data = '{0}'.format(i.value.decode())
                # get data only in json format and ignore other data
                try:
                    movie = json.loads(data)
                    print('===== ',movie['original_title'])
                    if len(list(collection.find({'_id':movie['_id']}))) == 0:
                        #collection.insert_one(movie)
                        movies.append(movie)
                        print('Movie validated and inserted')
                    else:
                        print('Movie validated and not inserted')
                except ValueError:
                    print('Another message that is not a movie')
                    print(data)
                    continue
        
except KeyboardInterrupt:
    print('Process stopped')
    