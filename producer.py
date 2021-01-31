import config
import tmdbsimple as tmdb
import requests
import json
from pykafka import KafkaClient


# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['movies']
# using the producer to link to topic
producer = topic.get_sync_producer()


# ====== STEP 1 : Get List of movies ============
print('Step 1: Loading movies list')
# Get list of moviews
url_search = f'https://api.themoviedb.org/3/discover/movie?primary_release_year=2020&sort_by=vote_average.desc&api_key={config.Config.API_KEY}&vote_count.gte=100'

# Call the API
print(url_search)
res = requests.get(url_search)
json_obj = res.json()
results = json_obj['results']

# Get paginated results from the API
page = int(json_obj['page'])
total_pages = int(json_obj['total_pages'])
i = 1
for i in range(page, 2):
    movies = []
    # Call >= 2 page
    if (i > 1):
        url_search = f'https://api.themoviedb.org/3/discover/movie?primary_release_year=2020&  sort_by=vote_average.desc&api_key={config.Config.API_KEY}&vote_count.gte=100'
        # Add page to get results
        url_search += f'&page={i}'
        res = requests.get(url_search)
        json_obj = res.json()
        results = json_obj['results']
    
    # Add only unique movies id
    for result in results:
        if result['id'] not in movies:
            movies.append(result['id'])
            print('Movie appended')

# Print results
for m in movies:
    print(m)

print('Step 2: Loading movies details')
# ====== STEP 2 : Get movies data ============
# Get data for each movie collected
#movies_list = []
for m_id in movies:
    url_search = f'https://api.themoviedb.org/3/movie/{m_id}?api_key={config.Config.API_KEY}'
    res = requests.get(url_search)
    result = res.json()
    movie = {}
    movie['_id'] = str(result['id'])
    movie['budget'] = result['budget']
    movie['genres'] = list(map(lambda m : m['name'], result['genres']))
    movie['original_language'] = result['original_language']
    movie['original_title'] = result['original_title']
    movie['popularity'] = result['popularity']
    movie['production_companies'] = list(map(lambda m : m['name'], result['production_companies']))
    movie['production_countries'] = list(map(lambda m : m['name'], result['production_countries']))
    movie['release_date'] = result['release_date']
    movie['revenue'] = result['revenue']
    movie['runtime'] = result['runtime']
    movie['status'] = result['status']
    movie['vote_average'] = result['vote_average']
    movie['vote_count'] = result['vote_count']
    #movies_list.append(movie)
    # Send data to consumer
    message = json.dumps(movie)
    producer.produce(message.encode('ascii'))
