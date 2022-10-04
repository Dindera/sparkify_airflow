from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
    COPY {}
    FROM '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id = "",
                 stage_table="",
                 s3_bucket_link="",
                 s3_data_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        # self.aws_credentials_id = aws_credentials_id
        self.stage_table = stage_table
        self.s3_bucket_link = s3_bucket_link
        self.s3_data_path = s3_data_path

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.stage_table))

        self.log.info("Copying data from S3 bucket to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_data_path
        )
        redshift.run(formatted_sql)
        
        self.log.info("Completed copying data to table {}".format(self.stage_table))




