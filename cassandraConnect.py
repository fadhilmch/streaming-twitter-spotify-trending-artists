from cassandra.cluster import Cluster
import datetime

class CassandraConnect:
    def __init__(self, keyspace):
        cluster = Cluster(['127.0.0.1'],port=9042)
        self.session = cluster.connect(keyspace ,wait_for_all_pools=True)
        self.session.execute('USE '+keyspace)

    def get_data(self, time_range, table):
        now = datetime.datetime.now().replace(microsecond=0).isoformat()
        last_hour = (datetime.datetime.now() - datetime.timedelta(minutes=time_range)).replace(microsecond=0).isoformat()
        rows = self.session.execute("SELECT artist, SUM(count) as total_count FROM "+table+" WHERE date >= '"+last_hour+"' AND date <= '"+now+"' GROUP BY artist ALLOW FILTERING")
        return rows



# SELECT artist, count(count) from TwitterTest GROUP BY artist;
