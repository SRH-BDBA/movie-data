from splinter import Browser
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
import requests
import config
import json
from pykafka import KafkaClient

# setting up the kafka client
client = KafkaClient(hosts='localhost:9092')
topic = client.topics['budget']
producer = topic.get_sync_producer()

# Open incognito browser tab
def open_browser():
    # Get geckodriver
    # https://github.com/mozilla/geckodriver/releases
    # Give exec permission
    # chmod +x geckodriver
    # geckdriver /user/bin
    return Browser('firefox',headless=False, incognito=True)

def scrapePage(url, producer):
    print('==== Visiting the webpage ====')
    # Visit the url
    browser.visit(url)
    # Wait for the page to render
    time.sleep(3)
    # Get response and get the html
    html= browser.html
    # Parase the response into a BS object
    soup= bs(html, "html.parser")
    # Get all rows from a table, starting from second row
    trows = soup.find_all("tr")[2:]
    for tr in trows:
        # Access all td's tags
        tds = tr.find_all('td')
        movie = {}
        # Create the dictionary in Python
        movie['title'] = tds[2].text
        # For numbers, remove everything before $ and get only the numbers
        movie['productionBudget'] = tds[3].text.split('$')[1]
        movie['domesticBudget'] = tds[4].text.split('$')[1]
        movie['worldwideGross'] = tds[5].text.split('$')[1]
        message = json.dumps(movie)
        try:
            # Send data to Kafka broker
            producer.produce(message.encode('ascii'))
            print(f"Movie: {movie['title']} successfully sent")
        except:
            # if any issue occurs, close the browser and run away!
            browser.close()
            continue
    print('==== Data successfully sent and closing the browser')
    


try:
    # URL to be scraped
    url = 'https://www.the-numbers.com/movie/budgets/all'
    # Open browser
    browser = open_browser()
    # Retrieve page with the requests module
    browser.visit(url)
    # Wait for the page to render
    time.sleep(3)
    # Get response and get the html
    html= browser.html
    # Parase the response into a BS object
    soup= bs(html, "html.parser")
    # Get pagination URLS
    #pages = soup.find("div", {"class":"pagination"})
    # Set any multiple of 100 + 1 i.e. 101, 201, 301 ...
    i = 101
    #a_links = pages.findAll('a')
    # Reading movies from 101 to 4901
    for i in range(i,5000,100):
        # Web page in the form: https://www.the-numbers.com/movie/budgets/all/101
        print(f"visiting: {url}/{str(i)}")
        # Scraping new link
        scrapePage(f"{url}/{str(i)}", producer)
        
    print(' ==== Read all pages ====')    
    browser.quit()
except KeyError as ke:
    print(ke.message)
    browser.quit()
# Stop the Kafka producer
producer.stop()