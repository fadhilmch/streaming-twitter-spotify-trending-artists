#!/usr/bin/python

import sys
import json
import pykafka
import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener

# Class for Kafka Push Listener
class KafkaPushListener(StreamListener):          
    def __init__(self):
        print("Publish data to topic: "+topic)
        self.client = pykafka.KafkaClient("localhost:9092")

        #Get Producer that has topic name is Twitter
        self.producer = self.client.topics[bytes(topic, "ascii")].get_producer()
  
    def on_data(self, data):
        #Producer produces data for consumer
        self.producer.produce(bytes(data, "ascii"))
#         print(len(data))
        print(data)
        return True
                                                                                                                                           
    def on_error(self, status):
        print(status)
        return True


if __name__ == '__main__':
    topic = sys.argv[1]
    word_filter = ['spotify com']
    language_filter = ['en']
    
    with open('credential.json') as f:
            data = json.load(f)
            api_key = data['twitter_api_key']
            api_secret = data['twitter_api_secret']
            token = data['twitter_token']
            token_secret = data['twitter_token_secret']
            
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(token, token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)
    twitter_stream = Stream(auth, KafkaPushListener())
    twitter_stream.filter(languages=language_filter, track=word_filter)

    