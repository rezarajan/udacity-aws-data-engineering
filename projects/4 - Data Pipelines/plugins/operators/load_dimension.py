from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 truncate=True,
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.truncate = truncate
        self.table = table
        self.sql = sql

    def execute(self, context):
        # Fetch conneciton from the Metastore backend
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Dimension operations truncate the table by default
        truncate_sql = f"""
        TRUNCATE {self.table};
        """

        dim_sql = f"""
        INSERT INTO {self.table} {self.sql};
        """

        # Generate the insert query
        formatted_sql = truncate_sql + dim_sql if self.truncate else dim_sql

        # Run the query using the redshift hook
        redshift.run(formatted_sql)
