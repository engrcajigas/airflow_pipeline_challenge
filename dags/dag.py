from datetime import datetime, timedelta 
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from plugins.helpers.sql_queries import SQLQueries
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.csv_to_sqlite import CSVtoSQLiteOperator
from plugins.operators.download_data_from_url import DownloadDataOperator
from plugins.operators.json_to_csv import JsonToCSVOperator
from plugins.operators.time_dimension_loader import TimeDimensionLoader
from plugins.operators.join_dataframes import ConcatTwoDataFramesOperator
from plugins.operators.drop_duplicates_csv import DropCSVDuplicatesOperator
from plugins.operators.test_data_operator import TestDataQualityOperator
from plugins.operators.calculate_product_overall_rating import CalculateOverallRatings

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

AIRFLOW_HOME = f'/home/airflow/airflow/'
dag = DAG('airflow_data_pipeline_to_transform_and_load_data_to_SQLite', 
          default_args=default_args,
          start_date=days_ago(1),
          #schedule_interval='0 */8 * * *')
          schedule_interval=None)  

start_operator = DummyOperator(
   task_id='Start_of_Execution',
   dag=dag 
)

end_operator = DummyOperator(
    task_id='End_of_Execution',  
    dag=dag
)

create_user_dimension_table = SqliteOperator(
    task_id='create_user_dimension_table',
    sqlite_conn_id='db_sqlite',
    sql=SQLQueries.users_table_create,
    dag=dag
)

create_product_dimension_table = SqliteOperator(
    task_id='create_product_dimension_table',
    sqlite_conn_id='db_sqlite',
    sql=SQLQueries.products_table_create, 
    dag=dag   
)

create_time_dimension_table = SqliteOperator(
    task_id='create_time_dimension_table',
    sqlite_conn_id='db_sqlite',
    sql=SQLQueries.time_table_create,
    dag=dag    
)

create_product_reviews_fact_table = SqliteOperator(
    task_id='create_product_reviews_fact_table',
    sqlite_conn_id='db_sqlite',
    sql=SQLQueries.product_reviews_table_create,
    dag=dag
)

download_meta_data_from_url = DownloadDataOperator(
    task_id="download_meta_data_from_url",
    url='http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Amazon_Instant_Video.json.gz',
    dag=dag
    
)

download_reviews_data_from_url = DownloadDataOperator(
    task_id="download_reviews_data_from_url",
    url='http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video_5.json.gz',
    dag=dag
)

transform_user_data_to_csv = JsonToCSVOperator(
    task_id="transform_user_data_to_csv",
    key_name='gz_downloaded_file',
    key_name_task_ids='download_reviews_data_from_url',
    tag='user',
    columns = ['reviewerID',
               'reviewerName'
               ],
    dag=dag
)

transform_review_data_to_csv = JsonToCSVOperator(
    task_id="transform_review_data_to_csv",
    key_name='gz_downloaded_file',
    key_name_task_ids='download_reviews_data_from_url',
    tag='review',
    columns = ['reviewerID',
               'reviewerName',
               'asin',
               'reviewText',
               'summary',
               'overall'
               ],
    dag=dag
)

transform_time_data_to_csv = JsonToCSVOperator(
    task_id='transform_time_data_to_csv',
    key_name='gz_downloaded_file',
    key_name_task_ids='download_reviews_data_from_url',
    tag='time',
    columns = ['unixReviewTime',
               'asin',
               'reviewerID'
               ],
    dag=dag
)

transform_meta_data_to_csv = JsonToCSVOperator(
    task_id='transform_meta_data_to_csv',
    key_name='gz_downloaded_file',
    key_name_task_ids='download_meta_data_from_url',
    tag='meta', 
    columns = ['asin',
               'price',
               'categories'
               ],
    dag=dag
)

time_dimension_data = TimeDimensionLoader(
    task_id='time_dimension_data',
    key_name='generated_csv_file',
    key_name_task_ids='transform_time_data_to_csv',
    dag=dag
)

merged_time_dataframes = ConcatTwoDataFramesOperator(
    task_id='merged_time_dataframes',
    key_name_one='generated_csv_file',
    key_name_one_task_ids='transform_time_data_to_csv',
    key_name_two='time_dimension_data_csv', 
    key_name_two_task_ids='time_dimension_data',
    on_column='unixReviewTime',
    csv_filename='data/merged_time_dataframes',
    dag=dag
)

drop_duplicate_userid = DropCSVDuplicatesOperator(
    task_id='drop_duplicate_userid',
    key_name='generated_csv_file',
    key_name_task_ids='transform_user_data_to_csv',
    on_column='reviewerID',
    tag='user',
    dag=dag
)

drop_duplicate_productid = DropCSVDuplicatesOperator(
    task_id='drop_duplicate_productid',
    key_name='generated_csv_file',
    key_name_task_ids='transform_meta_data_to_csv',
    on_column='asin',
    tag='product',
    dag=dag
)

calculate_overall_ratings = CalculateOverallRatings(
    task_id='calculate_overall_ratings',
    sqlite_conn_id='db_sqlite',
    key_name='generated_csv_file_no_duplicates',
    key_name_task_ids='drop_duplicate_productid',
    dag=dag
)

insert_user_data_to_sql = CSVtoSQLiteOperator(
    task_id='insert_user_data_to_sql',
    sqlite_conn_id='airflow.db',
    table_to_insert_data=SQLQueries.users_table,
    key_name='generated_csv_file_no_duplicates',
    key_name_task_ids='drop_duplicate_userid',
    dag=dag
)

insert_time_data_to_sql = CSVtoSQLiteOperator(
    task_id='insert_time_data_to_sql',
    sqlite_conn_id='airflow.db',
    table_to_insert_data=SQLQueries.time_table,
    key_name='merged_dataframe_csv',
    key_name_task_ids='merged_time_dataframes',
    dag=dag
)

insert_reviews_data_to_sql = CSVtoSQLiteOperator(
    task_id='insert_reviews_data_to_sql',
    sqlite_conn_id='airflow.db',
    table_to_insert_data=SQLQueries.product_reviews_table,
    key_name='generated_csv_file',
    key_name_task_ids='transform_review_data_to_csv',
    dag=dag
)

insert_product_data_to_sql = CSVtoSQLiteOperator(
    task_id='insert_product_data_to_sql',
    sqlite_conn_id='airflow.db',
    table_to_insert_data=SQLQueries.products_table,
    key_name='products_overall_ratings_csv',
    key_name_task_ids='calculate_overall_ratings',
    dag=dag
)


test_data_on_tables = TestDataQualityOperator(
    task_id='test_data_on_tables',
    sqlite_conn_id='db_sqlite',
    tables=[SQLQueries.users_table, SQLQueries.time_table, SQLQueries.products_table, SQLQueries.product_reviews_table],
    dag=dag    
)



# PIPELINE DEPENDENCIES
start_operator >> download_reviews_data_from_url 
start_operator >> download_meta_data_from_url >> create_product_dimension_table >> transform_meta_data_to_csv >> drop_duplicate_productid >> calculate_overall_ratings >> insert_product_data_to_sql
download_reviews_data_from_url >> create_user_dimension_table >> transform_user_data_to_csv >> drop_duplicate_userid >> insert_user_data_to_sql  
download_reviews_data_from_url >> create_time_dimension_table >>transform_time_data_to_csv >> time_dimension_data >> merged_time_dataframes >> insert_time_data_to_sql
download_reviews_data_from_url >> create_product_reviews_fact_table >> transform_review_data_to_csv >> insert_reviews_data_to_sql
insert_product_data_to_sql >> test_data_on_tables >> end_operator
insert_user_data_to_sql >> test_data_on_tables >>end_operator
insert_time_data_to_sql >> test_data_on_tables >> end_operator
insert_reviews_data_to_sql >> test_data_on_tables >> end_operator