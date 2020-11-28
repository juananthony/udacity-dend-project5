from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    log_entry = "[LoadFact]"
    insert_sql = """
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

        super(LoadFactOperator, self).__init__(*args, **kwargs)
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
        self.log.info(f"{self.log_entry} Loading {self.table} fact table in Redshift.")
        sql_formatted = LoadFactOperator.insert_sql.format(
            self.table,
            self.load_sql_stmt
        )
        redshift.run(sql_formatted)
        self.log.info(f"{self.log_entry} {self.table} fact table loaded.")