"""
Functions for querying Redshift DB
"""
import psycopg2
from src import utils

class RedshiftDatabase():
    def __init__(self):
        self.config = utils.get_db_config()
        # TODO track connections?

    def connect(self):
        """Get connection to Postgres database on Redshift cluster

        :returns: a psycopg2 connection to DB

        """
        conn = psycopg2.connect(
            dbname = self.config['NAME'],
            user = self.config['USER'],
            password = self.config['PASSWORD'],
            host = self.config['HOST'],
            port = self.config['PORT']
        )
        return conn


    def create_table(self, query):
        """Creates a table according to the passed query.

        :param query: str, PostgreSQL create table query

        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            if e.pgcode == "42P07":
                print("Table already exists:", e)
            else:
                raise
        else:
            conn.commit()
        finally:
            cur.close()
            conn.close()


    def execute_print(self, query, limit=None, format=lambda x: x):
        """Execute a query and print the results

        Note: limit affects how many lines are printed, but not how many
        are returned, so it is best to include the limit in the query.

        :param query: SQL query
        :param limit: max number of lines printed
        :param format: maps tuples from query to strings for printing

        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if limit:
                    for i, x in enumerate(cur.fetchall()):
                        if i > limit:
                            break
                        else:
                            print(format(x))
                else:
                    for x in cur.fetchall():
                        print(format(x))


    def print_load_errors(self):
        """Prints load errors
        """
        def format(a, b, c, d ,e):
            return f"{a:<8}{b:<23}{c:<7}{d:<19}{e:<50}"
        print(format('query', 'file', 'line', 'value', 'err_reason'))
        query = """
        select d.query, substring(d.filename,14,20),
        d.line_number as line,
        substring(d.value,1,16) as value,
        substring(le.err_reason,1,48) as err_reason
        from stl_loaderror_detail d, stl_load_errors le
        where d.query = le.query;
        """
        self.execute_print(query, format=lambda x: format(*tuple(x)))
