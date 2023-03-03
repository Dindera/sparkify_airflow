from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 aws_credentials_id= "aws_credentials",
                 redshift_conn_id = "",
                 retries_n = 0,
                 params = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.retries_n = retries_n
        self.params = params

        
    def execute(self, context):
        # Check Null Values in columns 
        # Check Count is not 0
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        songplay_table = self.params["songplays_table"]
        records = redshift.get_records(f"SELECT COUNT(*) FROM {songplay_table}")
        songplay_count = records[0][0]
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {songplay_table} is not equal to 10000")
        if songplay_count < 10000:
            raise ValueError(f"Data quality check failed. {songplay_table} contained {songplay_count}")
        self.log.info(f"Data quality on table {songplay_table} check passed with {songplay_count} records")
        