#!/usr/bin/env python3
"""
Script to run sample queries to test the data warehouse.

"""
import re
from prettytable import PrettyTable
from src import psql
####################
# QUERIES
####################
class Query():
    """Class to hold queries and a short description.
    """
    def __init__(self, summary, query):
        """Create a Query object

        :param summary: str, a short description of the query
        :param query: str, a SQL SELECT query

        """
        self.summary = summary
        self.query = query

    def columns(self):
        """Extract the column names of a SELECT query

        The column names appear as they would in the psql CLI.

        :returns: list of column names

        """
        pat = re.compile(r'(?:SELECT)(.+)(?:FROM|\n)', re.MULTILINE)
        m = re.search(pat, self.query).group(1)
        cols = [x.strip() for x in m.split(',')]

        def clean(x):
            if 'as' in x.lower():
                return x.lower().split('as')[-1].strip()
            elif '.' in x:
                return x.split('.')[-1]
            else:
                return x

        return [clean(x) for x in cols]


queries = []

# Show five songplays
songplays = "SELECT start_time, user_id, level, location FROM songplays LIMIT 5;"
queries.append(Query('Show five songplays', songplays))

# Show top 10 most popular songs
top_ten_songs = """
SELECT s.title, a.name, COUNT(sp.songplay_id) as num_plays
FROM songplays AS sp
JOIN artists AS a ON sp.artist_id = a.artist_id
JOIN songs AS s ON sp.song_id = s.song_id
GROUP BY s.title, a.name
ORDER BY num_plays DESC
LIMIT 10;
"""
queries.append(Query('Show top 10 most popular songs', top_ten_songs))

# Show five most popular artists
top_five_artists = """
SELECT a.name, COUNT(sp.songplay_id) as num_plays
FROM artists AS a JOIN songplays AS sp ON a.artist_id = sp.artist_id
GROUP BY a.name
ORDER BY num_plays DESC
LIMIT 5;
"""
queries.append(Query('Show five most popular artists', top_five_artists))

# Show 5 users with most songplays
top_five_listeners = """
SELECT u.first_name, u.last_name, COUNT(sp.songplay_id) AS num_plays
FROM songplays AS sp
JOIN users AS u ON u.user_id = sp.user_id
GROUP BY u.first_name, u.last_name
ORDER BY num_plays DESC
LIMIT 5;
"""
queries.append(Query('Show the top five users with the most songplays', top_five_listeners))

# Show average number of songplays stratified by tier

# Show number of users in each tier
users_by_level = "SELECT level, COUNT(user_id) FROM users GROUP BY level;"
queries.append(Query('Show the number of users in each level', users_by_level))

#####################
# END QUERIES
#####################

class QueryMenu():
    """Class to hold a list of queries, display the list,
    and print the queries based on list index (starting at 1).
    """
    def __init__(self, queries=[]):
        """Construtor

        :param queries: list of Query objects

        """
        self.queries = queries
        self.db = psql.RedshiftDatabase()

    def list_queries(self):
        """Print a list of available queries.

        The queries are numbered 1 through len(self.queries).

        """
        for i, query in enumerate(self.queries):
            print(f"{i + 1:<5}){query.summary}")

    def execute_query(self, i):
        """Print a query given its number as displayed
        by list_queries

        The query is printed in an ascii table.

        :param i: the number of the query (index in self.queries + 1)

        """
        if i < 1 or i > len(self.queries):
            raise IndexError(f"Index {i} invalid.")
        else:
            tbl = PrettyTable()
            tbl.field_names = self.queries[i - 1].columns()

            query = self.queries[i - 1].query
            with self.db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = [list(x) for x in cur.fetchall()]
            tbl.add_rows(rows)

            print('\n')
            print(tbl, '\n')


def main():
    """Show the user a list of queries, and prompt them
    to execute one.
    """
    qm = QueryMenu(queries)

    quit = False
    while not quit:
        qm.list_queries()
        print('\n')
        i = input("Enter the query number (q to quit): ")
        if i == 'q':
            quit = True
        else:
            try:
                qm.execute_query(int(i))
            except ValueError:
                print(f"Input 'q' or an integer from 1 to {len(qm.queries)}")

if __name__ == '__main__':
    main()
