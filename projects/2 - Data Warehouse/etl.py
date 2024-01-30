import configparser
import psycopg2
from pathlib import Path
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    # Load pararameters from dwh.cfg
    path = Path(__file__)
    ROOT_DIR = path.parent.absolute() # Use root path if calling script from a separate directory
    config_path = Path(ROOT_DIR, 'dwh.cfg')
    config = configparser.ConfigParser()
    config.read_file(open(config_path))

    conn = psycopg2.connect(
        dbname=config.get("CLUSTER", "DB_NAME"),
        user=config.get("CLUSTER", "DB_USER"),
        password=config.get("CLUSTER", "DB_PASSWORD"),
        host=config.get("CLUSTER", "HOST"),
        port=config.get("CLUSTER", "DB_PORT")
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()