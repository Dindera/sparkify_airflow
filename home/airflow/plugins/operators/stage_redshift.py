from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION 'us-west-2'
    JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="aws_credentials",
                 stage_table="",
                 s3_bucket="",
                 s3_data = "",
                 json = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.stage_table = stage_table
        self.s3_bucket = s3_bucket
        self.json = json
        self.s3_data = s3_data

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("DELETE FROM {}".format(self.stage_table))

        self.log.info("Copying data from S3 bucket to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_data)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.stage_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)
        
        self.log.info("Completed copying data to table {}".format(self.stage_table))




