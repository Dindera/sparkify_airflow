import logging
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
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

        
    def execute(self, context):
        # Check Null Values in columns 
        # Check Count is not 0
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
           
            record_count = records[0][0]
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} contains {record_count}")
            elif table == "songs":
                check_null = redshift.get_records(f"SELECT COUNT(*) FROM {table} WHERE artistid = NULL")
                null_count = check_null[0][0]
                if null_count < 0:
                   raise ValueError(f"Data quality check failed. artistid column in {table} contains null {null_count} values ")
            elif record_count < 1:
                raise ValueError(f"Data quality check failed. {table} contained {record_count}")
            else:
                self.log.info(f"Data quality on table {table} check passed with {record_count} records")
            