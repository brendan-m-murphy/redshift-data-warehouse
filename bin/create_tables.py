from src import cluster, psql
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cluster = cluster.Cluster()
    if cluster.status() == 'paused':
        cluster.resume()
        cluster.wait()

    db = psql.RedshiftDatabase()
    with db.connect() as conn:
        with conn.cursor() as cur:
            drop_tables(cur, conn)
            create_tables(cur, conn)



if __name__ == "__main__":
    main()
