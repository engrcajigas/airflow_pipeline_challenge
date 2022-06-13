from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults

import pandas as panda
from datetime import date

class ConcatTwoDataFramesOperator(BaseOperator):
    
    ui_color = '#D0C9C0'
    
    @apply_defaults
    def __init__(
        self,
        key_name_one,
        key_name_one_task_ids,
        key_name_two,
        key_name_two_task_ids, 
        on_column,
        csv_filename,
        *args, 
        **kwargs  
    ):
        super(ConcatTwoDataFramesOperator, self).__init__(*args, **kwargs)
        self.key_name_one = key_name_one
        self.key_name_one_task_ids = key_name_one_task_ids 
        self.key_name_two = key_name_two
        self.key_name_two_task_ids = key_name_two_task_ids 
        self.on_column = on_column
        self.csv_filename = csv_filename
    
    def join_df(self, dataframe_one_csv, dataframe_two_csv):
        dataframe_one = panda.read_csv(dataframe_one_csv)
        dataframe_two = panda.read_csv(dataframe_two_csv)
        date_today = date.today().strftime("%Y%m%d")
        #merged_dataframe = panda.merge(dataframe_one, dataframe_two,
        #                               on=self.on_column, how='inner')
        merged_dataframe = panda.concat([dataframe_one, dataframe_two], axis=1)
        merged_dataframe.to_csv("{}_{}.csv".format(self.csv_filename, date_today), index=False)
        
        merged_dataframe_csv = "{}_{}.csv".format(self.csv_filename, date_today)
        return merged_dataframe_csv
    
    def execute(self, context):
        dataframe_one_csv = context['ti'].xcom_pull(key=self.key_name_one, task_ids=self.key_name_one_task_ids)
        dataframe_two_csv = context['ti'].xcom_pull(key=self.key_name_two, task_ids=self.key_name_two_task_ids)
        merged_dataframe_csv = self.join_df(dataframe_one_csv, dataframe_two_csv)
        context['ti'].xcom_push(key='merged_dataframe_csv', value=merged_dataframe_csv)