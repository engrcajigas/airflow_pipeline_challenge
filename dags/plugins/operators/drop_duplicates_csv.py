from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults

from datetime import date
import pandas as panda

class DropCSVDuplicatesOperator(BaseOperator):
    
    ui_color = '#ECE5C7'
    
    @apply_defaults
    def __init__(
        self,
        key_name, 
        key_name_task_ids,
        on_column,
        tag, 
        *args, 
        **kwargs
    ):
        super(DropCSVDuplicatesOperator, self).__init__(*args, **kwargs)
        self.key_name = key_name
        self.key_name_task_ids = key_name_task_ids
        self.on_column = on_column
        self.tag = tag
    
    def drop_duplicates(self, csv_filename):
        dataframe = panda.read_csv(csv_filename).drop_duplicates(subset=[self.on_column], keep='last')
        dataframe.to_csv('{}/{}_{}_{}.csv'.format('data', self.tag, 'no_duplicates', date.today().strftime("%Y%m%d")), index=False)
        
        print('**************************************')
        print(dataframe.loc[dataframe.duplicated(), :])
        
        generated_csv_file_no_duplicates = "{}/{}_{}_{}.csv".format('data', self.tag, 'no_duplicates',date.today().strftime("%Y%m%d"))
        return generated_csv_file_no_duplicates
        
    def execute(self, context):
        csv_filename = context['ti'].xcom_pull(key=self.key_name, task_ids=self.key_name_task_ids)
        generated_csv_file_no_duplicates = self.drop_duplicates(csv_filename)
        
        context['ti'].xcom_push(key='generated_csv_file_no_duplicates', value=generated_csv_file_no_duplicates)
        