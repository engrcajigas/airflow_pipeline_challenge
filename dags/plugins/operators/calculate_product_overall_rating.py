import sqlite3
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from datetime import date
import pandas as panda

class CalculateOverallRatings(BaseOperator):
    
    ui_color = '#E7FBBE'
    
    @apply_defaults
    def __init__(
        self,
        sqlite_conn_id,
        key_name, 
        key_name_task_ids,
        *args, 
        **kwargs
    ):
        super(CalculateOverallRatings, self).__init__(*args, **kwargs)
        self.sqlite_conn_id = sqlite_conn_id
        self.key_name = key_name
        self.key_name_task_ids = key_name_task_ids
    
    def execute(self, context):
        date_today = date.today().strftime("%Y%m%d")

        sqlite_hook = SqliteHook(sqlite_conn_id=self.sqlite_conn_id)
        
        # OPTIMIZE THIS: Build index/indexes on this table to optimize data access
        # and avoid the use of GROUP BY on this query.
        get_average_rating_query=("""
            SELECT asin, AVG(overall) AS [avg_rating] 
                FROM product_reviews GROUP BY asin;
        """)
        
        records=sqlite_hook.get_records(get_average_rating_query)
        
        dataframe_one_csv = context['ti'].xcom_pull(key=self.key_name, task_ids=self.key_name_task_ids)
        
        dataframe_two_csv = "{}/{}_{}.csv".format('data', 'overall_rating_computation', date_today)
        data_two = panda.DataFrame(records, columns=['asin', 'overall_rating'])
        data_two.to_csv("{}/{}_{}.csv".format('data', 'overall_rating_computation', date_today), index=False)
        
        dataframe_one = panda.read_csv(dataframe_one_csv)
        dataframe_two = panda.read_csv(dataframe_two_csv)
        
        merged_dataframe = panda.merge(dataframe_one, dataframe_two, how='outer', on='asin')
        merged_dataframe.to_csv("{}/{}_{}.csv".format('data', 'products_overall_ratings', date_today), index=False)
        products_overall_ratings_csv = "{}/{}_{}.csv".format('data', 'products_overall_ratings', date_today)
        
        context['ti'].xcom_push(key='products_overall_ratings_csv', value=products_overall_ratings_csv)
        
