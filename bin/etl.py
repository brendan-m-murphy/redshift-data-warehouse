"""ETL script for data warehouse

Loads JSON files into staging tables,
then populates a star schema from the
staging tables.

Optional arguements:
- -t: run on restricted set of data
- -f: run on full set of data without prompt

If no optional arguments are present, you will
be asked to confirm that you wish to load *all*
of the data.

"""
import argparse
from src import psql
from src.sql_queries import copy_table_queries, insert_table_queries, test_copy_table_queries


def load_staging_tables(cur, conn):
    """Run copy queries for staging tables.

    :param cur: psycopg connection
    :param conn: psycopg cursor

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def test_load_staging_tables(cur, conn):
    """Run copy queries for staging tables on small subset of data.

    :param cur: psycopg connection
    :param conn: psycopg cursor

    """
    for query in test_copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Run insert queries to load data into star schema

    :param cur: psycopg connection
    :param conn: psycopg cursor

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def full_etl(db):
    """Run ETL on all JSON data

    :param db: RedshiftDatabase object (from src.psql)

    """
    with db.connect() as conn:
        with conn.cursor() as cur:
            print('* Loading staging tables (all data)')
            load_staging_tables(cur, conn)
            print('* Populating star schema')
            insert_tables(cur, conn)


def test_etl(db):
    """Run ETL on small subset of JSON data

    :param db: RedshiftDatabase object (from src.psql)

    """
    with db.connect() as conn:
        with conn.cursor() as cur:
            print('* Loading staging tables (test data)')
            test_load_staging_tables(cur, conn)
            print('* Populating star schema')
            insert_tables(cur, conn)


def main():
    """ETL script

    Run using `etl` on command line, if installed by setup.py.
    Otherwise, run as a python script.

    Optional arguments:
    - `-t` load a small subset of data for testing
    - `-y` run the script on the full set of data, without prompt

    If run without optional arguments, you will be asked to confirm
    that you wish to load all of the data, which could take > 4 hours.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test', help="load subset of data",
                        action="store_true")
    parser.add_argument('-y', '--yes', help="skip y/n prompt",
                        action="store_true")
    args = parser.parse_args()
    db = psql.RedshiftDatabase()

    if args.test:
        test_etl(db)
    elif args.yes:
        full_etl(db)
    else:
        x = input("Do you want to load *all* of the data? [y/n] ")
        if x == 'y':
            full_etl(db)
        else:
            print("Run `etl -t` to load only the test data.")


if __name__ == "__main__":
    main()
