import tweepy 
import time
import os
import re
from elasticsearch import Elasticsearch
  
# Fill the X's with the credentials obtained by  
# following the above mentioned procedure. 
consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_key = os.environ['TWITTER_ACCESS_KEY']
access_secret = os.environ['TWITTER_ACCESS_SECRET']
  
# Function to extract tweets
    
def get_centroid(coordinates):
    x, y = zip(*coordinates)
    l = len(x)
    return sum(x)/l, sum(y)/l
        
def convert_tweet_to_document(tweet):
    text = tweet.text
    id = tweet.id
    ts = tweet.created_at
    body = {'event_timestamp': ts}
    
    if tweet.entities['hashtags']:
        text = remove_by_indices(text, tweet.entities['hashtags'])
        tags = [h['text'] for h in tweet.entities['hashtags']]
        body.update({'text': text, 'tags': tags})
    else:
        body.update({'text': text})
    
    coordinates = parse_location_data(tweet)
    body.update(coordinates)
        
    return id, body

def remove_by_indices(text, indices):
    '''
    Strips out unwanted text using the start and stop indices
    '''
    
    # get all the start and stop indices of unwanted text
    slices = [{'i':h['indices'][0], 'j':h['indices'][1]} for h in indices]
    
    # sort from last to first so none of the indices change
    slices = sorted(slices, key=lambda s: s['i'], reverse=True)
    
    for s in slices:
        text = text[:s['i']] + text[s['j']:]
        
    return ' '.join(text.split())
    
def parse_location_data(tweet):
    if tweet.place:
        coordinates = tweet.place.bounding_box.coordinates[0]
        lon, lat = get_centroid(coordinates)
        country_code = tweet.place.country_code
        full_name = tweet.place.full_name
        coordinates = {
            'name': full_name, 
            'country_code': country_code,
            'location': {
                'lon': lon,
                'lat': lat,
            }
        }
    elif tweet.coordinates:
        coordinates = {
            'location': {
                'lat': tweet.coordinates['coordinates'][1],
                'lon': tweet.coordinates['coordinates'][0],
            }
        }
    elif tweet.geo:
        coordinates = {
            'location': {
                'lat': tweet.coordinates['coordinates'][0],
                'lon': tweet.coordinates['coordinates'][1],
            }
        }
    else:
        coordinates = {}
        
    return coordinates 

if __name__ == '__main__':
    
    bonsai = os.environ['BONSAI_URL']
    auth = re.search('https\:\/\/(.*)\@', bonsai).group(1).split(':')
    host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '').split(':')[0]

    # Connect to cluster over SSL using auth for best security:
    es_header = [{
     'host': host,
     'port': 443,
     'use_ssl': True,
     'http_auth': (auth[0],auth[1])
    }]

    # Instantiate the new Elasticsearch connection:
    es = Elasticsearch(es_header)
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
    auth.set_access_token(access_key, access_secret) 
  
    api = tweepy.API(auth)
    
    # poll the API
    print("App started! Polling the twitter API...")
    
    while True:
        tweets = api.user_timeline("metajosie")
        for tweet in tweets:
            id, doc = convert_tweet_to_document(tweet)
            res = es.index(index="twitter-metajosie", doc_type="tweets", id=id, body=doc)
        
            # after we load the tweet, delete it
            # this is a nice work around for twitter not letting
            # you post the same tweet more than once per hour
        
            api.destroy_status(id)
            
        time.sleep(5)