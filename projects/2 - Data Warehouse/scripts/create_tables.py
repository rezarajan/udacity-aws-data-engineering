import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from helpers import LoadConfig


def drop_tables(cur, conn):
    """
    Executes the queries to drop all tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes the queries to create the staging tables, and the dimensional model tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = LoadConfig(autoload=True)

    conn = psycopg2.connect(
        dbname=config.get("CLUSTER", "DB_NAME"),
        user=config.get("CLUSTER", "DB_USER"),
        password=config.get("CLUSTER", "DB_PASSWORD"),
        host=config.get("CLUSTER", "HOST"),
        port=config.get("CLUSTER", "DB_PORT")
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()