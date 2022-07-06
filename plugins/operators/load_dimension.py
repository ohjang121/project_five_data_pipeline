from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    Operator to insert data from staging table to dimension tables
    Same code as LoadFactOperator, only difference in using self.is_full_refresh parameter
    Params:
        table: Destination table to load data into
        select_query: SQL query to select from staging table
        is_full_refresh: Boolean to indicate append or full refresh insertion
        redshift_conn_id: Redshift connection id based on Airflow connections set up
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 select_query,
                 is_full_refresh,
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select_query = select_query
        self.is_full_refresh = is_full_refresh # Improvement: may need to restrict input to be boolean
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id) # may need to replace to (postgres_conn_id=self.redshift_conn_id)
        
        # truncate table before insertion
        if self.is_full_refresh:
            self.log.info(f'Clearing data from destination Redshift dimension table {self.table}...')
            try:
                redshift_hook.run(f'TRUNCATE TABLE {self.table}')
                self.log.info(f'Successfully truncated table {self.table}')
            except Exception as e:
                self.log.error(e)
        
        # insert data from staging table to dimension table
        insert_query = f'''
                    INSERT INTO {self.table}
                    {self.select_query}
                    '''
        
        self.log.info(f'Query to insert data from staging table to dimension table {self.table}: {insert_query}')

        try:
            redshift_hook.run(insert_query)
            self.log.info(f'Successfully inserted data from staging table to dimension table {self.table}!')
        except Exception as e:
            self.log.error(e)
        

