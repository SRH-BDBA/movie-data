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
# Specify the number of records to insert into the DB
load_batch = 50
try:
    # Access the kafka consumer for the movies topic
    consumer = topic.get_simple_consumer(
        consumer_group='test-consumer-group', 
        auto_offset_reset=OffsetType.LATEST)
    # Process should be listening all the time to receive new messages
    while True:
        # Initial validation in case we have pending records to insert
        if len(movies) > 0:
                collection.insert_many(movies)
                print(f'{len(movies)} records successfully inserted')
                # Empty list or movies
                movies = []
                continue
        # in case we have new incomming messages
        for i in consumer:
            # Make sure is a new message
            if i is not None:
                # Verify if we have a full loading list
                if len(movies) == load_batch:
                    # Insert multiple records and 
                    collection.insert_many(movies)
                    print(f'{len(movies)} records successfully inserted')
                    # Empty list or movies
                    movies = []

                data = '{0}'.format(i.value.decode())
                # get data only in json format and ignore other data
                try:
                    movie = json.loads(data)
                    print('===== ',movie['original_title'])
                    # Validate if each movie already exists in the DB and add it to the batch load
                    if len(list(collection.find({'_id':movie['_id']}))) == 0:
                        #collection.insert_one(movie)
                        movies.append(movie)
                        print('(*) Movie validated and inserted')
                    else:
                        print('(x) Movie validated and not inserted')
                except ValueError:
                    print('Another message that is not a movie')
                    print(data)
                    continue
        
except KeyboardInterrupt:
    print('Process stopped')
    