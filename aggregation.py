import pymongo
import config

conn = config.MONGO_URL
client = pymongo.MongoClient(conn)
db = client["movies"]
collection1 = db.movies_collection
collection2 = db.budget_collection
collection3 = db.aggregated_collection


data = list(collection1.aggregate( [

  { "$lookup" : {
     "from" : "budget_collection",
     "localField" : "original_title",
     "foreignField" : "title",
      "as": "aggregate"
    }
  },
    {
    "$unwind":{
        "path": "$aggregate"
    }
    }
]))


collection3.insert_many(data)
print(f'{len(data)} is the number of movies inserted in the aggregated_collection')
