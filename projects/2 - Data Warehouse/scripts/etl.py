import logging
import argparse
import psycopg2
from pathlib import Path
from sql_queries import copy_table_queries, insert_table_queries
from helpers import LoadConfig


def load_staging_tables(cur, conn):
    """
    Executes the queries responsible for loading data into staging tables from S3.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
def insert_tables(cur, conn):
    """
    Executes the queries responsible for transforming staging table data into the dimensional model tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Runs either the loading queries, or the transformation queries.

    Args:
    - all: Runs the load and transform queries, respectively
    - load: Runs the load queries only
    - etl: Runs the transformation queries only

    Example:
    ```
    python etl.py all
    ```
    """
    logging.basicConfig(level=logging.INFO)  # Set the logging level

    # Load pararameters from dwh.cfg
    config = LoadConfig(autoload=True)

    conn = psycopg2.connect(
        dbname=config.get("CLUSTER", "DB_NAME"),
        user=config.get("CLUSTER", "DB_USER"),
        password=config.get("CLUSTER", "DB_PASSWORD"),
        host=config.get("CLUSTER", "HOST"),
        port=config.get("CLUSTER", "DB_PORT")
    )
    cur = conn.cursor()

    parser = argparse.ArgumentParser(description='Execute ETL operations.')
    parser.add_argument('operation', choices=['load', 'etl'], nargs='?', default='all', help='Specify the operation to perform (load, etl, or all).')

    args = parser.parse_args()

    if args.operation in ['all', 'load']:
        logging.info('Loading staging data.')
        load_staging_tables(cur, conn)
        logging.info('Staging tables loaded.')

    if args.operation in ['all', 'etl']:
        logging.info('Performing ETL.')
        insert_tables(cur, conn)
        logging.info('ETL completed.')

    conn.close()


if __name__ == "__main__":
    main()