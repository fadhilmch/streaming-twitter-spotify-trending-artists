from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext
import sys
import json
import re
import datetime as dt
from dateutil.parser import parse

import spotify_caller

# define spark context and streaming context
def define_context():
    conf = SparkConf().setMaster("local[*]").setAppName("twitter-artist-count").set("spark.cassandra.connection.host", "127.0.0.1")
    sc = CassandraSparkContext.getOrCreate(conf = conf)
    sc.setCheckpointDir("./checkpoints")
    ssc = StreamingContext(sc, 30)
    return ssc

# define kafka receiver (using receiver-less approach)
def kafka_receiver(ssc, topic):
    topic = [sys.argv[1]]
    kafkaParams = {"metadata.broker.list": "localhost:9092",
           "zookeeper.connect": "localhost:2181",
           "group.id": "kafka-spark-streaming",
           "zookeeper.connection.timeout.ms": "1000"}
    return KafkaUtils.createDirectStream(ssc, topic, kafkaParams)

# extract track id from url in tweet
def get_track_id(urls):
    if(len(urls))>0:
        track_id = re.findall('(?<=track\/)[^.?]*',urls)
        if track_id:
            return track_id[0]
        return False
    else:
        return False

if __name__ == "__main__":
    # need spark-streaming-kafka and pyspark-cassandra package to run
    # run the file with spark-submit --packages anguenot/pyspark-cassandra:2.4.0,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 --conf spark.cassandra.connection.host=127.0.0.1 kafkaStreaming.py <topic_name>

    # connect to Cassandra and create keyspace + table if don't exist
    keyspace = "spotify"
    table = "artistshare"
    cluster = Cluster(['127.0.0.1'],port=9042)
    session = cluster.connect(wait_for_all_pools=True)
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 1};" )
    session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + "." +table + " (created_at timestamp, artist text, count int, PRIMARY KEY(artist, created_at));")

    # define streaming context and kafka receiver
    ssc = define_context()
    topic = [sys.argv[1]]
    kafkaStream = kafka_receiver(ssc, topic)
    Spotify = spotify_caller.SpotifyAPI()

    # extract track id for each incoming tweet and use the track id to get artist name using Spotify API
    tweets = kafkaStream.map(lambda value: json.loads(value[1]))
    #tweets.count().map(lambda count: "Tweets in this batch: %s" % count).pprint()
    #tweets.pprint()

    track_ids = tweets.map(lambda tweet: {"track_id":get_track_id\
                (tweet["expanded_url"]), "created_at":tweet["created_at"]}).\
                filter(lambda track: track["track_id"]!=False)
    #track_ids.pprint()
    #track_ids.count().map(lambda count: "Tracks in this batch: %s" % count).pprint()
    artists = track_ids.map(lambda track: {"artist":Spotify.get_track_information\
    (track["track_id"]), "created_at":track["created_at"]}).\
    filter(lambda artist: artist["artist"] != None)

    # construct rows for insertion to Cassandra
    rows = artists.map(lambda artist: {"created_at":parse(artist["created_at"]).\
            replace(microsecond=0).isoformat(), "artist":artist["artist"], "count": 1})

    # write to Cassandra
    #rows.pprint()
    try:
        rows.foreachRDD(lambda x: x.saveToCassandra(keyspace, table))
    except Exception as e:
        print(f'Error Message: {e}')

    ssc.start()
    ssc.awaitTermination()
