"""
Drops tables (if they exist) from the Redshift cluster database,
then creates staging tables and tables for the star schema.

Run this script before running `etl.py` to clear the tables.

"""
from src import cluster, psql
from src.sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Run queries to drop all tables

    :param cur: psycopg connection
    :param conn: psycopg cursor

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Run queries to create all tables

    :param cur: psycopg connection
    :param conn: psycopg cursor

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Script to drop and create tables

    If installed by setup.py, run on the command line
    with `create-tables`. Otherwise, run as a python script.

    If you run this script while the cluster is
    paused, it will automatically resume the cluster
    before running.

    """
    cluster_ = cluster.Cluster()
    if cluster_.status() == 'paused':
        cluster_.resume()
        print('* Waiting for cluster to resume.')
        cluster_.wait()

    db = psql.RedshiftDatabase()
    with db.connect() as conn:
        with conn.cursor() as cur:
            print('* Dropping tables')
            drop_tables(cur, conn)
            print('* Creating tables')
            create_tables(cur, conn)


if __name__ == "__main__":
    main()
