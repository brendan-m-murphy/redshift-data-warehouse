import psycopg2
import aws_utils
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

    if aws_utils.redshift_properties()['ClusterStatus'] == 'paused':
        aws_utils.resume_cluster()
        aws_utils.wait_for_cluster_available()

    with aws_utils.get_connection() as conn:
        with conn.cursor() as cur:
            drop_tables(cur, conn)
            create_tables(cur, conn)



if __name__ == "__main__":
    main()
