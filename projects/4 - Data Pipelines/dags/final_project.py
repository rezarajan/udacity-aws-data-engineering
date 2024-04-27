import pendulum

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from test_suite import TestSuite

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage the event log data to Redshift.
    # YYYY/MM partitioning is enable via templating on the s3_key
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='aws-dend-airflow/project-4',
        s3_key='log-data/{{ execution_date.strftime("%Y/%m") }}',
        json='log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='aws-dend-airflow/project-4',
        s3_key='song-data',
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        redshift_conn_id='redshift_test',
        sql=TestSuite.row_count_sql.format(table='songs'),
        test_function=TestSuite.row_count_test
    )


final_project_dag = final_project()
