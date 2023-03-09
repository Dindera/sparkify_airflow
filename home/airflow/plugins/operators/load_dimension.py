from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 append_mode = False,
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.append_mode = append_mode
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_mode == False:
            self.log.info('Deleting data from table {}'.format(self.table))
            redshift.run(f'DELETE FROM {self.table}')
            self.log.info('Data from table {} deleted.'.format(self.table))
        
        self.log.info('Loading data into {}'.format(self.table))
        redshift.run(sql=self.sql)

        self.log.info('Loaded data into {}'.format(self.table))
