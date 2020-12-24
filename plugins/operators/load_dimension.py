from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    
    """This function is used to load data into dimension tables from staging tables.
    
    Parameters:
    redshift_conn_id: Connection Id of the Airflow to redshift database
    table: The destianation table to be updated
    sql_statement: The sql statement which is used to fill the date into the dimension table from the staging tables
    
    Returns: None
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 udpdate_type= "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table= table
        self.redshift_conn_id= redshift_conn_id
        self.sql_statement= sql_statement
        self.udpdate_type= udpdate_type

    def execute(self, context):
        self.log.info('LoadDimensionOperator implementation to be started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.udpdate_type == 'overwrite':
            insert_query= 'truncate {}; insert into {} ({})'.format(self.table,self.table,self.sql_statement)
        elif self.udpdate_type == 'insert':
            insert_query = 'insert into {} ({})'.format(self.table, self.sql_statement)
        redshift.run(insert_query)
