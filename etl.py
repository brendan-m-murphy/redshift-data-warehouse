import psycopg2
import aws_utils
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in reversed(copy_table_queries):
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    with aws_utils.get_connection() as conn:
        with conn.cursor() as cur:
            load_staging_tables(cur, conn)
#            insert_tables(cur, conn)


if __name__ == "__main__":
    main()
