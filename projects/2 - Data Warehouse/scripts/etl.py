import logging
import psycopg2
from pathlib import Path
from sql_queries import copy_table_queries, insert_table_queries
from helpers import LoadConfig


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
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
    
    # Load staging table data
    load_staging_tables(cur, conn)
    logging.info('Staging tables loaded')

    insert_tables(cur, conn)
    logging.info('ETL completed.')

    conn.close()


if __name__ == "__main__":
    main()