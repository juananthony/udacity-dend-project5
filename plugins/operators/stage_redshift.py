from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    log_entry = "[StageToRedshiftOperator]"
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """
    delete_sql = """
    DELETE FROM {};
    """
    s3_path_base = "s3://{}/{}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 extra_params="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_params = extra_params

    def execute(self, context):
        """
        Copy data from a S3 bucket to a Redshift table
        :param context: Airflow dictionary with context.
        :return:
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"{self.log_entry} Deleting tables in redshift.")
        redshift.run(self.delete_sql.format(self.table))

        self.log.info(f"{self.log_entry} Inserting from S3 bucket ({self.s3_bucket} to {self.table} table in Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = self.s3_path_base.format(self.s3_bucket, rendered_key)
        sql_formatted = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_params
        )

        self.log.info(f"{self.log_entry} Executing {sql_formatted}")
        redshift.run(sql_formatted)
        self.log.info(f"{self.log_entry} {self.table} copied from {self.s3_bucket} bucket")
