import sqlite3
from airflow.models import BaseOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.decorators import apply_defaults

import sqlite3
import pandas as panda 

class CSVtoSQLiteOperator(BaseOperator):
    
    ui_color = '#8CC0DE'
    
    @apply_defaults
    def __init__(
        self,
        sqlite_conn_id,
        table_to_insert_data,
        key_name, 
        key_name_task_ids,
        *args, **kwargs
    ):
        super(CSVtoSQLiteOperator, self).__init__(*args, **kwargs)
        self.sqlite_conn_id = sqlite_conn_id
        self.table_to_insert_data = table_to_insert_data
        self.key_name = key_name
        self.key_name_task_ids = key_name_task_ids

    def execute(self, context):
        csv_filename=context['ti'].xcom_pull(key=self.key_name, task_ids=self.key_name_task_ids)
        connection=sqlite3.connect(self.sqlite_conn_id)
        c=connection.cursor() 
        df=panda.read_csv('{}'.format(csv_filename))
        self.log.info("Writing records of {} to {} table".format(csv_filename, self.table_to_insert_data))
        try:
            df.to_sql(self.table_to_insert_data, connection, if_exists='append', index=False, method='multi')
        except Exception as e:
            self.log.error('Error is encountered: {}'.format(e))
        
        
        

