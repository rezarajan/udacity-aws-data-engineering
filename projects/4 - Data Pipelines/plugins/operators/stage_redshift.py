from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift_connection',
                 aws_credentials_id='aws_credentials',
                 table='',
                 s3_bucket='',
                 s3_key='{{ execution_date.strftime("%Y/%m") }}',
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter

    def execute(self, context):
        # Fetch connections from the metastore backend
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Truncate the staging table
        self.log.info('Clearing data from the destination Redshift table')
        redshift.run(f'TRUNCATE {self.table}')

        # Format sql strings
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.ignore_headers,
            self.delimiter
        )

        # Copy the data to Redshift
        self.log.info('Copying data from S3: {s3_path} to Redshift')
        redshift.run(formatted_sql)

