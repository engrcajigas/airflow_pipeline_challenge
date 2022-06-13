import shutil
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults

from datetime import date
import pandas as panda
import gzip

class JsonToCSVOperator(BaseOperator):
    
    ui_color = '#94DAFF'
    
    @apply_defaults
    def __init__(
        self,
        key_name, 
        key_name_task_ids,
        tag,
        columns,
        *args,
        **kwargs
    ):
        super(JsonToCSVOperator, self).__init__(*args, **kwargs)
        self.key_name = key_name
        self.key_name_task_ids = key_name_task_ids
        self.tag = tag
        self.columns = columns
        
    def transform_to_valid_json(self, filename):
        csv_file = filename.split('.gz')[0]
        with gzip.open(filename, 'rb') as f_in:
            with open(csv_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        with open(csv_file, 'r') as input:
            lines = input.readlines()
            last_line = lines[-1]
            with open('data/transformed_{}.json'.format(date.today().strftime("%Y%m%d")), 'w') as output:
                output.write('[')
                for line in lines:
                    change_line = line.replace("'", '"')
                    if line is last_line:
                        output.write(change_line+']')
                    else:
                        output.write(change_line+',')
                        
    
    def json_to_csv(self, filename):
        csv_file = filename.split('.')[0].replace(" ", "_")
        dataframe = panda.read_json('data/transformed_{}.json'.format(date.today().strftime("%Y%m%d")))
        my_data = panda.DataFrame(dataframe, columns=self.columns)
        print("Converting {} to csv".format(csv_file))
        my_data.to_csv("{}_{}_{}.csv".format(csv_file, self.tag, date.today().strftime("%Y%m%d")), index=False)
    
        generated_csv_file="{}_{}_{}.csv".format(csv_file, self.tag, date.today().strftime("%Y%m%d"))
        return generated_csv_file
        
    
    def extract_data_to_csv(self, filename):
        csv_file = filename.split('.')[0].replace(" ", "_")
        try:
            dataframe = panda.read_json(filename, lines=True, compression='gzip')
            my_data = panda.DataFrame(dataframe, columns=self.columns)
            print("Converting {} to csv".format(filename))
            my_data.to_csv("{}_{}_{}.csv".format(csv_file, self.tag, date.today().strftime("%Y%m%d")), index=False)
        except Exception as e:
            print("Error encountered: {}".format(e)) 
        
        generated_csv_file="{}_{}_{}.csv".format(csv_file, self.tag, date.today().strftime("%Y%m%d"))                       
        return generated_csv_file
        
                        
    def execute(self, context):
        filename = context['ti'].xcom_pull(key=self.key_name, task_ids=self.key_name_task_ids)
        print(filename)
        if "meta" in filename:
            self.transform_to_valid_json(filename)
            generated_csv_file=self.json_to_csv(filename)
        else:
            generated_csv_file = self.extract_data_to_csv(filename)
        
        context['ti'].xcom_push(key='generated_csv_file', value=generated_csv_file)