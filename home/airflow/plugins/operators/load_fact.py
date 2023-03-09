from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table_name = "",
                 append_mode = False,
                 sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.append_mode = append_mode
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_mode == False:
            self.log.info('Deleting data from {}'.format(self.table_name))
            redshift.run(f'DELETE FROM {self.table_name}')
            self.log.info(f'Data in {self.table_name} deleted')
        self.log.info('Loading data into {}'.format(self.table_name))
        redshift.run(sql=self.sql)

        self.log.info('Loaded data into {}'.format(self.table_name))
