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
# producer = topic.get_producer(delivery_reports=True, min_queued_messages=1)



# ====== STEP 1 : Get List of movies ============
print('Step 1: Loading movies list')
# Get list of moviews

for i in range(2014,2019, 1):
    release_year = i
    sort_by = 'vote_average.desc'
    url_search = f'https://api.themoviedb.org/3/discover/movie?primary_release_year={release_year}&sort_by={sort_by}&api_key={config.API_KEY}'

    # Call the API
    print(url_search)
    res = requests.get(url_search)
    json_obj = res.json()
    results = json_obj['results']

    # Get paginated results from the API
    page = int(json_obj['page'])
    #page = 1
    total_pages = int(json_obj['total_pages'])
    if total_pages >= 10: 
        total_pages = 10
    #total_pages = 2
    i = 1

    print(f"Loading list of movies and getting their ids... Total Pages:{total_pages}")

    movies = []
    for i in range(page, total_pages):
        print(f'Page: {i}')
        # Call >= 2 page
        if (i > 1):
            url_search = f'https://api.themoviedb.org/3/discover/movie?primary_release_year={release_year}&sort_by={sort_by}&api_key={config.API_KEY}'
            # Add page to get results
            url_search += f'&page={i}'
            res = requests.get(url_search)
            json_obj = res.json()
            results = json_obj['results']
        
        # Add only unique movies id
        for result in results:
            if result['id'] not in movies:
                movies.append(result['id'])
                

    # Print results
    print(f'Total movies added: {len(movies)}')
    for m in movies:
        print(m)

    print('Step 2: Loading movies details')
    # ====== STEP 2 : Get movies data ============
    # Get data for each movie collected
    #movies_list = []
    for m_id in movies:
        url_search = f'https://api.themoviedb.org/3/movie/{m_id}?api_key={config.API_KEY}&append_to_response=credits'
        res = requests.get(url_search)
        result = res.json()
        movie = {}
        try:
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
            movie['cast'] = list(map(lambda m : {'department': m['known_for_department'], 'name': m['original_name'],'popularity': m['popularity']}, result['credits']['cast']))
            movie['crew'] = list(map(lambda m : {'department': m['department'],'name': m['original_name'],'popularity': m['popularity']}, result['credits']['crew']))
            message = json.dumps(movie)
            print(f" ======== \n {movie['original_title']}")
            producer.produce(message.encode('ascii'))
        except KeyError:
            continue
        
        

producer.stop()