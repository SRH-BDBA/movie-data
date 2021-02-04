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
collection = db.budget_collection

movies = []
# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['budget']

try:
    consumer = topic.get_simple_consumer(
        consumer_group='test-consumer-group', 
        auto_offset_reset=OffsetType.LATEST)
    # Process should be listening all the time to receive new messages
    while True:
        for i in consumer:
            if i is not None:
                data = '{0}'.format(i.value.decode())
                # get data only in json format and ignore other data
                
                movie = json.loads(data)
                print('===== ',movie['title'])
                # Data cleaning
                movie['productionBudget'] = movie['productionBudget'].replace(',','')
                movie['domesticBudget'] = movie['domesticBudget'].replace(',','')
                movie['worldwideGross'] = movie['worldwideGross'].replace(',','')
                # Cast as int
                movie['productionBudget'] = float(movie['productionBudget'])
                movie['domesticBudget'] = float(movie['domesticBudget'])
                movie['worldwideGross'] = float(movie['worldwideGross'])
                if len(list(collection.find({'title':movie['title']}))) == 0:
                    collection.insert_one(movie)
                    print(f"{movie['title']} validated and inserted")
                else:
                    print(f"{movie['title']} validated and NOT inserted")
                
            else:
                continue
        
except KeyboardInterrupt:
    print('Process stopped')
    