"""ETL script for data warehouse

Loads JSON files into staging tables,
then populates a star schema from the
staging tables.

Optional arguements:
- `test`: run on restricted set of data
- `y`: run on full set of data without prompt

If no optional arguments are present, you will
be asked to confirm that you wish to load *all*
of the data.

"""
import psycopg2
import sys
import aws_utils
from sql_queries import copy_table_queries, insert_table_queries, test_copy_table_queries


def load_staging_tables(cur, conn):
    for query in reversed(copy_table_queries):
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def test_load_staging_tables(cur, conn):
    for query in reversed(test_copy_table_queries):
        cur.execute(query)
        conn.commit()


def main(*args):
    if len(args) == 1:
        answer = input("Do you want to load all of the data, a test set, or quit? [all/test/quit] ")
        if answer == 'all':
            with aws_utils.get_connection() as conn:
                with conn.cursor() as cur:
                    load_staging_tables(cur, conn)
                    insert_tables(cur, conn)
        elif answer == 'test':
            with aws_utils.get_connection() as conn:
                with conn.cursor() as cur:
                    test_load_staging_tables(cur, conn)
                    insert_tables(cur, conn)
    elif len(args) == 2:
        if args[1] == 'y':
            with aws_utils.get_connection() as conn:
                with conn.cursor() as cur:
                    load_staging_tables(cur, conn)
                    insert_tables(cur, conn)
        elif args[1] == 'test':
            with aws_utils.get_connection() as conn:
                with conn.cursor() as cur:
                    test_load_staging_tables(cur, conn)
                    insert_tables(cur, conn)
        else:
            raise Exception("Invalid argument. Valid arguments are 'test' or 'y'.")
    else:
        raise Exception("Too many arguments. Valid arguments are 'test' or 'y'.")



if __name__ == "__main__":
    main(*sys.argv)
