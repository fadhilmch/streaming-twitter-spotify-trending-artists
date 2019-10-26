from cassandra.cluster import Cluster

if __name__ == "__main__":
    cluster = Cluster(['127.0.0.1'],port=9042)
    session = cluster.connect('avg_space',wait_for_all_pools=True)
    session.execute('USE avg_space')
    rows = session.execute('SELECT * FROM avg')
    for row in rows:
        print(row.word, row.count)
