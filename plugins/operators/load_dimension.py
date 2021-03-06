from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    log_entry = "[Load Dimension]"
    insert_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        """
        Executes the insert sql statement in redshift.
        :param context: Airflow dictionary with context.
        :return:
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"{self.log_entry} Loading dimension table {self.table} in Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.table,
            self.load_sql_stmt
        )
        redshift.run(formatted_sql)
        self.log.info(f"{self.log_entry} {self.table} loaded.")
