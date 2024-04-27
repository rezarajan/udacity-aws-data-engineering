from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql='',
                 test_function=lambda x: True,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.test_function = test_function

    def execute(self, context):
        # Fetch connections from the metastore backend
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Run test query
        records = redshift.get_records(self.sql)

        # Run test function and raise error if failed
        result = False
        try:
            result = self.test_function(records)
        except AssertionError as e:
            print(e)

        if not result:
            raise AssertionError('Test failed!')
