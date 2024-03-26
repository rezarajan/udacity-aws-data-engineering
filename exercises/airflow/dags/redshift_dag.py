import pendulum
import logging

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from scripts.common import sql_statements

@dag(start_date=pendulum.now())
def load_data_to_redshift():
    # Load data via S3
    
    @task
    def load_task():
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift") # load redshift connection
        redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(aws_connection.login, aws_connection.password))

    # Create tables
    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
    # ETL
    location_traffic_task = PostgresOperator(
        task_id="calculate_location_traffic",
        postgres_conn_id="redshift",
        sql=sql_statements.LOCATION_TRAFFIC_SQL_CREATE 
    )

    # DAG Order
    load_task = load_task()
    create_tables_task >> load_task
    load_task >> location_traffic_task


s3_to_redshift_dag = load_data_to_redshift()
