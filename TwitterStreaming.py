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
        # Producer produces data for consumer
        json_object = json.loads(data)
        # Filter tweets so only tweets containing link are sent to kafka producer
        # Send only created at and contained urls of the tweets
        if len(json_object['entities']['urls']) > 0:
            data = {'created_at':json_object['created_at'], 'expanded_url':json_object['entities']['urls'][0]['expanded_url']}
            data = json.dumps(data)
            self.producer.produce(bytes(data, "ascii"))
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

    twitter_stream = Stream(auth, KafkaPushListener())
    twitter_stream.filter(languages=language_filter, track=word_filter)
