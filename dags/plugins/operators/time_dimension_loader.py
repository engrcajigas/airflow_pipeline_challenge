from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults

from datetime import datetime, date

class TimeDimensionLoader(BaseOperator):
    
    ui_color = '#E6BA95'
    
    @apply_defaults
    def __init__(
        self,
        key_name, 
        key_name_task_ids,
        *args,
        **kwargs
    ):
        super(TimeDimensionLoader, self).__init__(*args, **kwargs)
        self.key_name = key_name
        self.key_name_task_ids = key_name_task_ids
        
    
    def transform_unix_time(self, csv_filename):
        with open(csv_filename, 'r') as input:
            lines = input.readlines()[1:]
            with open('data/time_dimension_{}.csv'.format(date.today().strftime("%Y%m%d")), 'w') as output:
                output.write('{},{},{}\n'.format('year', 'month', 'day'))
                for line in lines:
                    unix_time = line.split(',')[0]
                    transformed_time = datetime.fromtimestamp(float(unix_time))
                    year = transformed_time.strftime('%Y')
                    month = transformed_time.strftime('%m')
                    day = transformed_time.strftime('%d')
                    output.write('{},{},{}\n'.format(year, month, day))
        
        time_dimension_data_csv = 'data/time_dimension_{}.csv'.format(date.today().strftime("%Y%m%d"))
        return time_dimension_data_csv
            
    def execute(self, context):
        csv_filename = context['ti'].xcom_pull(key=self.key_name, task_ids=self.key_name_task_ids)
        time_dimension_data_csv = self.transform_unix_time(csv_filename)
        context['ti'].xcom_push(key='time_dimension_data_csv', value=time_dimension_data_csv)
        