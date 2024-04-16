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
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift_connection',
                 aws_credentials_id='aws_credentials',
                 table='',
                 s3_bucket='',
                 s3_key='{{ execution_date.strftime("%Y/%m") }}',
                 json='auto'
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json

    def execute(self, context):
        # Fetch connections from the metastore backend
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(
            self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate the staging table
        self.log.info(
            f'Clearing data from the destination Redshift table {self.table}')
        redshift.run(f'TRUNCATE {self.table}')

        # Format sql strings
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        json_path = f's3://{self.s3_bucket}/{self.json}' if self.json != 'auto' else 'auto'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            json_path
        )

        # Copy the data to Redshift
        self.log.info(f'Copying data from S3: {s3_path} to Redshift')
        redshift.run(formatted_sql)
