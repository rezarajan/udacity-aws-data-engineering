import pendulum

from airflow.decorators import dag, task_group
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

    start_operator = DummyOperator(task_id='begin_execution')

    # Stage the event log data to Redshift.
    # YYYY/MM partitioning is enable via templating on the s3_key
    @task_group
    def staging_group():
        """Staging tasks"""
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

    @task_group
    def load_facts():
        """Load fact tables"""
        load_songplays_table = LoadFactOperator(
            task_id='load_songplays_fact_table',
            redshift_conn_id='redshift',
            table='songplays',
            sql=SqlQueries.songplay_table_insert
        )

    @task_group
    def load_dimensions():
        "Load dimension tables"
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

    @task_group
    def run_tests():
        """Run all data quality checks"""
        for table in ['songplays', 'users', 'songs', 'artists', 'time']:
            DataQualityOperator(
                task_id=f'run_data_quality_check_{table}',
                redshift_conn_id='redshift',
                sql=TestSuite.row_count_sql,
                table=table,
                test_function=TestSuite.row_count_test
            )

    end_operator = DummyOperator(task_id='end_execution')

    start_operator >> staging_group() >> load_facts(
    ) >> load_dimensions() >> run_tests() >> end_operator


final_project_dag = final_project()
