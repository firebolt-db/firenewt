#!/usr/bin/python3
"""Generate Trickle DML queries for the FireNEWT benchmark.

Produces files for the following scenarios:
- INSERT:
    - 1 row, 100 queries
    - 10 rows, 100 queries
    - 100 rows, 100 queries
    - 1000 rows, 100 queries
- UPDATE:
    - 1 row, 100 queries
- DELETE:
    - 1 row, 100 queries

This script queries the table `uservisits` in the database selected by environment variable
FB_DATABASE, and generates queries for DML on the rows in that specific table. This is required
for running benchmarks on different tables/data than the one the existing DML scripts were
generated for.
"""
from abc import ABC, abstractmethod
import csv
import datetime
import firebolt.db
from firebolt.client.auth import ClientCredentials
import os
import pandas as pd
import random
import sys
import textwrap


current_dir = os.path.dirname(os.path.abspath(__file__))
module_dir = os.path.join(current_dir, "..", "packages")
sys.path.append(module_dir)

# This table will be queried to find rows to target for DML.
SOURCE_TABLE = "uservisits"
# The S3 prefix containing the files needed to ingest a fresh copy of the base table.
# Each scenario will have a query that loads this data into a new table for DML to act on.
S3_PREFIX = "s3://firebolt-benchmarks-requester-pays-{{region}}/firenewt/100gb/uservisits/"


INSERT_BENCHMARK_SCENARIOS = {
    "1row-100queries": {
        "rows_per_query": 1,
        "num_queries": 100,
    },
    "10rows-100queries": {
        "rows_per_query": 10,
        "num_queries": 100,
    },
    "100rows-100queries": {
        "rows_per_query": 100,
        "num_queries": 100,
    },
    "1000rows-100queries": {
        "rows_per_query": 1000,
        "num_queries": 100,
    },
}

UPDATE_BENCHMARK_SCENARIOS = {
    "1row-100queries": {
        "num_queries": 100,
    },
}

DELETE_BENCHMARK_SCENARIOS = {
    "1row-100queries": {
        "num_queries": 100,
    },
}


def escape_string(s):
    """Escapes single-quotes in SQL strings."""
    return s.replace("'", "''")


def printrows(rows):
    """Useful for debug purposes."""
    for row in rows:
        print(row)


class BenchmarkWriter(ABC):
    """Abstract base class for writing benchmark scenarios.

    This class handles the overall flow of writing scenarios and also provides methods for generating queries, but
    subclasses implement formatting the scenario and storing it to disk as required by the specific benchmark system.
    """

    @abstractmethod
    def _write_insert_scenario(self, name, rows_per_query, num_queries, unused_rows):
        pass

    @abstractmethod
    def _write_update_scenario(self, name, rows_per_query, num_queries, unused_rows):
        pass

    @abstractmethod
    def _write_delete_scenario(self, name, rows_per_query, num_queries, unused_rows):
        pass

    def _make_insert_query(self, table_name, rows):
        newline_if_multiple = "\n" if len(rows) > 1 else ""
        spaces = "    " if len(rows) > 1 else " "
        query = f"INSERT INTO {table_name} VALUES" + newline_if_multiple
        query += ",\n".join([f"{spaces}('{r[0]}', '{escape_string(r[1])}', '{r[2]}', {r[3]}, '{r[4]}', '{r[5]}', '{r[6]}', '{escape_string(r[7])}', {r[8]})" for r in rows])
        query += newline_if_multiple + ";"
        return query

    def _make_update_query(self, table_name, row):
        return textwrap.dedent(f"""\
             UPDATE {table_name} SET
               sourceip = REGEXP_REPLACE(sourceip, '.[0-9]*$','.999'),
               destinationurl = CONCAT(destinationurl, '_'),
               visitdate = visitdate + 1,
               adrevenue = adrevenue + 1,
               useragent = CONCAT(useragent, '_'),
               countrycode = CONCAT(countrycode, '_'),
               languagecode = CONCAT(languagecode, '_'),
               searchword = CONCAT(searchword, '_'),
               duration = duration + 1
             WHERE visitdate = '{row[2]}' AND destinationurl = '{escape_string(row[1])}' AND sourceip = '{row[0]}';""")

    def _make_delete_query(self, table_name, row):
        return f"DELETE FROM {table_name} WHERE visitdate = '{row[2]}' AND destinationurl = '{escape_string(row[1])}' AND sourceip = '{row[0]}';"

    def write_insert_scenarios(self, scenarios, rows_for_insert_generator):
        """Write all of the INSERT scenarios, using rows from parameter unused_rows."""
        for name, params in scenarios.items():
            self._write_insert_scenario(name, params["rows_per_query"], params["num_queries"],
                                        rows_for_insert_generator)

    def write_update_scenarios(self, scenarios, target_row_generator):
        """Write all of the UPDATE scenarios, with queries that target rows from target_row_generator."""
        for name, params in scenarios.items():
            self._write_update_scenario(name=name, rows_per_query=1, num_queries=params["num_queries"],
                                        target_row_generator=target_row_generator)

    def write_delete_scenarios(self, scenarios, target_row_generator):
        """Write all of the DELETE scenarios, with queries that target rows from target_row_generator."""
        for name, params in scenarios.items():
            self._write_delete_scenario(name=name, rows_per_query=1, num_queries=params["num_queries"],
                                        target_row_generator=target_row_generator)


class FireNEWTBenchmarkWriter(BenchmarkWriter):
    """Implementation of BenchmarkWriter for writing the scenario in the FireNEWT format."""
    DIR = os.path.join(current_dir, "..", "SQL", "trickle_ingestion")
    WORKLOAD_REPLAY_INSERT_FILENAME_TEMPLATE = "insert_{rows_per_query}r_{num_queries}q.csv"
    WORKLOAD_REPLAY_UPDATE_FILENAME_TEMPLATE = "update_{rows_per_query}r_{num_queries}q.csv"
    WORKLOAD_REPLAY_DELETE_FILENAME_TEMPLATE = "delete_{rows_per_query}r_{num_queries}q.csv"

    def _write_scenario(self, filename, table_name, query_id_prefix, query_generator):
        """Generate and write a scenario (sequence of queries) to a CSV file.

        Args:
            filename: The name of the file to write to.
            table_name: The name of the table to use in the queries.
            query_id_prefix: The prefix to use for the query IDs. The query number is used as the suffix.
            query_generator: A generator that yields the queries to write. `_write_scenario` will add queries until
                this generator is exhausted.
        """
        path = os.path.join(self.DIR, filename)

        # This timestamp variable is increased for each query. It's stored in a list so that it can be modified by the
        # write_row function.
        start_ts = [datetime.datetime(2024, 9, 17, 0, 0, 0)]

        csv_rows = []

        def write_row(query_id, query_text):
            end_ts = start_ts[0] + datetime.timedelta(seconds=59)
            csv_rows.append(
                {"query_start_ts": start_ts[0], "query_end_ts": end_ts, "query_id": query_id, "query_text": query_text})
            start_ts[0] = start_ts[0] + datetime.timedelta(minutes=1)

        write_row('system_prep_1_drop_ext', f'DROP TABLE IF EXISTS external_{table_name};')
        write_row('system_prep_2_create_ext',
                  f'CREATE EXTERNAL TABLE external_{table_name}'
                  ' (sourceip TEXT NOT NULL, destinationurl TEXT NOT NULL, visitdate PGDATE NOT NULL,'
                  ' adrevenue REAL NOT NULL, useragent TEXT NOT NULL, countrycode TEXT NOT NULL,'
                  ' languagecode TEXT NOT NULL, searchword TEXT NOT NULL, duration INTEGER NOT NULL)'
                  f' OBJECT_PATTERN = \'*\' TYPE = (PARQUET) URL = \'{S3_PREFIX}\''
                  ' {{public_requester_pays_bucket_credentials}};')
        write_row('system_prep_3_drop', f'DROP TABLE IF EXISTS {table_name};')
        write_row('system_prep_4_create',
                  f'CREATE FACT TABLE {table_name}'
                  ' (sourceip TEXT NOT NULL, destinationurl TEXT NOT NULL, visitdate PGDATE NOT NULL,'
                  ' adrevenue REAL NOT NULL, useragent TEXT NOT NULL, countrycode TEXT NOT NULL,'
                  ' languagecode TEXT NOT NULL, searchword TEXT NOT NULL, duration INTEGER NOT NULL)'
                  ' PRIMARY INDEX visitdate, destinationurl, sourceip;')
        write_row('system_prep_5_ingest', f'INSERT INTO {table_name} SELECT * FROM external_{table_name};')
        write_row('system_prep_6_vacuum', f'VACUUM {table_name};')
        write_row('system_prep_7_checksum', f'SELECT checksum(*) FROM {table_name};')

        for i, query in enumerate(query_generator):
            write_row(f'{query_id_prefix}{i+1}', query)

        write_row('system_cleanup_1_drop_table', f'DROP TABLE IF EXISTS {table_name};')
        write_row('system_cleanup_2_drop_et', f'DROP TABLE IF EXISTS external_{table_name};')

        df = pd.DataFrame(csv_rows, columns=["query_start_ts", "query_end_ts", "query_id", "query_text"])
        df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)


    def _write_insert_scenario(self, name, rows_per_query, num_queries, rows_for_insert_generator):
        table_name = f'insert_{rows_per_query}r_{num_queries}q'

        def query_generator():
            """Generates quantity `num_queries` insert queries."""
            for _ in range(num_queries):
                query = self._make_insert_query(table_name=table_name,
                                                rows=[next(rows_for_insert_generator) for _ in range(rows_per_query)])
                yield query

        self._write_scenario(
            filename=self.WORKLOAD_REPLAY_INSERT_FILENAME_TEMPLATE.format(rows_per_query=rows_per_query,
                                                                          num_queries=num_queries),
            table_name=table_name,
            query_id_prefix=f'insert_{rows_per_query}r_{num_queries}q_',
            query_generator=query_generator())

    def _write_update_scenario(self, name, rows_per_query, num_queries, target_row_generator):
        table_name = f'update_{rows_per_query}r_{num_queries}q'

        def query_generator():
            """Generates quantity `num_queries` update queries."""
            for i in range(num_queries):
                query = self._make_update_query(table_name=table_name, row=next(target_row_generator))
                yield query

        self._write_scenario(
            filename=self.WORKLOAD_REPLAY_UPDATE_FILENAME_TEMPLATE.format(rows_per_query=rows_per_query,
                                                                          num_queries=num_queries),
            table_name=table_name,
            query_id_prefix=f'update_{rows_per_query}r_{num_queries}q_',
            query_generator=query_generator())


    def _write_delete_scenario(self, name, rows_per_query, num_queries, target_row_generator):
        table_name = f'delete_{rows_per_query}r_{num_queries}q'
        def query_generator():
            """Generates quantity `num_queries` delete queries."""
            for _ in range(num_queries):
                query = self._make_delete_query(table_name=table_name, row=next(target_row_generator))
                yield query

        self._write_scenario(
            filename=self.WORKLOAD_REPLAY_DELETE_FILENAME_TEMPLATE.format(rows_per_query=rows_per_query,
                                                                          num_queries=num_queries),
            table_name=table_name,
            query_id_prefix=f'delete_{rows_per_query}r_{num_queries}q_',
            query_generator=query_generator())


def get_randomly_chosen_row_from_table_generator(cursor, batch_size):
    """A generator function that selects unique rows randomly from the table, that are also unique to the PI value."""

    already_chosen_rows = set()

    row_pool = []
    while True:
        # Randomly get approximately batch_size * 10 rows. This method of selection should return a set of rows that are
        # randomly located in storage. The number of rows is non-deterministic, so it may require multiple queries to
        # get enough rows.
        if not row_pool:
            print("Querying for a random set of rows to use as DML targets...")
            cursor.execute(f"SELECT * FROM {SOURCE_TABLE} WHERE random() < CAST({batch_size * 10} AS FLOAT) / (SELECT count(*) FROM {SOURCE_TABLE});")
            row_pool = cursor.fetchall()
            # Shuffle the rows, so that if we use fewer than the full set of rows, we are unlikely to get ones located
            # close together in storage. (The order of the rows returned by the query is likely to correlate with the
            # order they are stored in.)
            random.shuffle(row_pool)

            print(f"Retrieved {len(row_pool)} rows to draw from.\n")

        row = row_pool.pop()

        row_tuple = tuple(row)
        if row_tuple in already_chosen_rows:
            continue

        # We want PI values that only have 1 corresponding row, so that the DML operations will affect only one row when
        # targeting the PI values.
        cursor.execute(f"""
                     SELECT count(*) FROM {SOURCE_TABLE} WHERE
                     sourceip = '{row[0]}' AND
                     destinationurl = '{escape_string(row[1])}' AND
                     visitdate = '{row[2]}';
                     """, skip_parsing=True)
        matches = cursor.fetchone()[0]
        if matches != 1:
            continue

        already_chosen_rows.add(row_tuple)

        yield row


def create_firebolt_connection():
    conn = firebolt.db.connect(
        auth=ClientCredentials(
            client_id=os.environ["FB_CLIENT_ID"],
            client_secret=os.environ["FB_CLIENT_SECRET"],
        ),
        account_name=os.environ["FB_ACCOUNT"],
        engine_name=os.environ["FB_ENGINE"],
        database=os.environ["FB_DATABASE"],
        api_endpoint=os.environ["FB_API"],
    )
    return conn


def generate_queries():
    print("Connecting to Firebolt...\n")

    connection = create_firebolt_connection()
    cursor = connection.cursor()

    print("Generating scenarios...\n")

    w = FireNEWTBenchmarkWriter()

    print("Creating INSERT scenarios...\n")

    num_rows_to_generate_for_inserts = sum([params["rows_per_query"] * params["num_queries"] for params in INSERT_BENCHMARK_SCENARIOS.values()])
    print(f"Generating {num_rows_to_generate_for_inserts} rows for inserts...\n")
    cursor.execute(textwrap.dedent(f"""\
        SELECT
          sourceip, destinationurl,
          (CAST((SELECT MAX(visitdate) + 1 FROM {SOURCE_TABLE}) AS PGDATE)
           + (visitdate - (SELECT min(visitdate) FROM {SOURCE_TABLE}))) visitdate,
          adrevenue, useragent, countrycode, languagecode, searchword, duration
        FROM {SOURCE_TABLE}
        LIMIT {num_rows_to_generate_for_inserts};"""))
    rows_for_inserts = cursor.fetchall()
    if len(rows_for_inserts) != num_rows_to_generate_for_inserts:
        print(f"Expected {num_rows_to_generate_for_inserts} rows, but got {len(rows_for_inserts)} rows. "
              "Perhaps the table is too small.")
        sys.exit(1)

    w.write_insert_scenarios(INSERT_BENCHMARK_SCENARIOS, iter(rows_for_inserts))

    print("Creating UPDATE scenarios...\n")

    num_random_rows = (sum([params["num_queries"] for params in UPDATE_BENCHMARK_SCENARIOS.values()])
                       + sum([params["num_queries"] for params in DELETE_BENCHMARK_SCENARIOS.values()]))
    random_row_chooser = get_randomly_chosen_row_from_table_generator(cursor, num_random_rows)

    w.write_update_scenarios(UPDATE_BENCHMARK_SCENARIOS, random_row_chooser)

    print("Creating DELETE scenarios...\n")

    w.write_delete_scenarios(DELETE_BENCHMARK_SCENARIOS, random_row_chooser)

    print("Done.\n")

    connection.close()


def check_env_variable(var_name):
    value = os.environ.get(var_name)
    if value is None:
        print(f"Environment variable '{var_name}' is not set.")
        return False
    return True


if __name__ == "__main__":
    env_variables_set = True
    env_variables_set = env_variables_set and check_env_variable("FB_CLIENT_ID")
    env_variables_set = env_variables_set and check_env_variable("FB_CLIENT_SECRET")
    env_variables_set = env_variables_set and check_env_variable("FB_ACCOUNT")
    env_variables_set = env_variables_set and check_env_variable("FB_ENGINE")
    env_variables_set = env_variables_set and check_env_variable("FB_DATABASE")
    env_variables_set = env_variables_set and check_env_variable("FB_API")
    if env_variables_set:
        generate_queries()
