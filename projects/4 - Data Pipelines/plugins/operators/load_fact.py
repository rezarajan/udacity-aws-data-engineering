from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    # Fact tables should only allow append-sytle functionality
    facts_sql = """
    INSERT INTO {table} {sql};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        # Fetch connections from the metastore backend
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Generate the insert query
        formatted_sql = self.facts_sql.format(
            table=self.table,
            sql=self.sql
        )
        # Run the query using the redshfit hook
        redshift.run(formatted_sql)
