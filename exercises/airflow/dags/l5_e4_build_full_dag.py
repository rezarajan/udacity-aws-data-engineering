import pendulum

from airflow.decorators import dag,task

from facts_calculator import FactsCalculatorOperator
from has_rows import HasRowsOperator
from s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator

#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
@dag(start_date=pendulum.now())
def full_pipeline():

    copy_trips_task = S3ToRedshiftOperator(
        task_id = 'copy_trips_task',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        table = 'trips',
        s3_bucket = 'aws-dend-airflow',
        s3_key = 'data-pipelines/divvy/unpartitioned/divvy_trips_2018'
    )

    check_trips = HasRowsOperator(
        task_id = 'check_trips_task',
        redshift_conn_id = 'redshift',
        table = 'trips'
    )

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in RedShift. The fact column should
#       be `tripduration` and the groupby_column should be `bikeid`
#
    calculate_facts = FactsCalculatorOperator(
        task_id = 'create_fact_table_task',
        redshift_conn_id = 'redshift',
        origin_table = 'trips',
        destination_table = 'f_trips',
        groupby_column = 'bikeid',
        fact_column = 'tripduration'
    )

    # Define task ordering
    copy_trips_task >> check_trips >> calculate_facts

full_pipeline_dag = full_pipeline()
