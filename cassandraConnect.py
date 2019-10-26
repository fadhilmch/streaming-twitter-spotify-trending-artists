from cassandra.cluster import Cluster
import datetime

class CassandraConnect:
    def __init__(self):
        cluster = Cluster(['127.0.0.1'],port=9042)
        self.session = cluster.connect('twitter_keyspace',wait_for_all_pools=True)
        self.session.execute('USE twitter_keyspace')

    def get_data(self, time_range):
        now = datetime.datetime.now().replace(microsecond=0).isoformat()
        last_hour = (datetime.datetime.now() - datetime.timedelta(minutes=time_range)).replace(microsecond=0).isoformat()
        rows = self.session.execute("SELECT artist, SUM(count) FROM TwitterTest WHERE date >= '"+last_hour+"' AND date <= '"+now+"' GROUP BY artist ALLOW FILTERING")
        return rows



# select artist from TwitterTest where date >= '2019-10-26T09:46:17' ALLOW FILTERING;
