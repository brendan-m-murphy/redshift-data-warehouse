"""ETL script for data warehouse

Loads JSON files into staging tables,
then populates a star schema from the
staging tables.

Optional arguements:
- `test`: run on restricted set of data
- `all`: run on full set of data without prompt

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


def full_etl():
    with aws_utils.get_connection() as conn:
        with conn.cursor() as cur:
            print('* Loading staging tables (all data)')
            load_staging_tables(cur, conn)
            print('* Populating star schema')
            insert_tables(cur, conn)


def test_etl():
    with aws_utils.get_connection() as conn:
        with conn.cursor() as cur:
            print('* Loading staging tables (test data)')
            test_load_staging_tables(cur, conn)
            print('* Populating star schema')
            insert_tables(cur, conn)


def main(*args):
    if len(args) == 1:
        answer = input("Do you want to load all of the data, a test set, or quit? [all/test/quit] ")
        if answer == 'all':
            full_etl()
        elif answer == 'test':
            test_etl()
    elif len(args) == 2:
        if args[1] == 'all':
            full_etl()
        elif args[1] == 'test':
            test_etl()
        else:
            raise Exception("Invalid argument. Valid arguments are 'test' or 'all'.")
    else:
        raise Exception("Too many arguments. Valid arguments are 'test' or 'all'.")



if __name__ == "__main__":
    main(*sys.argv)
