from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
# from pyspark_cassandra import streaming
import os
import sys
import json
import re

import spotipy
import json
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyAPI:
    def __init__(self):
        with open('credential.json') as f:
            data = json.load(f)
            client_id = data['SPOTIFY_CLIENT_ID']
            client_secret = data['SPOTIFY_CLIENT_SECRET']
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    def get_track_information(self,id):
        return self.sp.track(id)['artists'][0]['name']

def get_track_id(urls):
    if(len(urls))>0:
        track_id = re.findall('(?<=track\/)[^.?]*',urls[0]['expanded_url'])
        if track_id:
            return track_id[0]
        return False
    else:
        return False
    
if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 pyspark-shell'
    Spotify = SpotifyAPI()
    
    # Create Streaming Context and set batch interval
    conf = SparkConf().setMaster("local[*]").setAppName("twitter-sentiment")
    sc = SparkContext.getOrCreate(conf = conf)
#     sc.setLogLevel("WARN")
    sc.setCheckpointDir("./checkpoints")
    ssc = StreamingContext(sc, 5)

    brokers = "localhost:9092"
    topic = [sys.argv[1]]
    kafkaParams = {"metadata.broker.list": "localhost:9092",
           "zookeeper.connect": "localhost:2181",
           "group.id": "kafka-spark-streaming",
           "zookeeper.connection.timeout.ms": "1000"}
    kafkaStream = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
    tweet = kafkaStream.map(lambda value: json.loads(value[1])). \
        map(lambda json_object: get_track_id(json_object["entities"]["urls"])). \
        filter(lambda url: url!=False)
    artist = tweet.map(lambda track: Spotify.get_track_information(track))
    
    # Do data cleaning and sentiment analysis
    artist.pprint()
    
#     tweet.saveToCassandra("twitter_keyspace", "Twitter")
    ssc.start()
    ssc.awaitTermination()
