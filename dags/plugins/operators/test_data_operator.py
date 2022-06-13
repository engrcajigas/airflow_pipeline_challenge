from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

class TestDataQualityOperator(BaseOperator):
    
    ui_color = '#CDF0EA'
    
    @apply_defaults
    def __init__(
        self,
        sqlite_conn_id,
        tables=[],
        *args, 
        **kwargs
    ):
        super(TestDataQualityOperator, self).__init__(*args, **kwargs)
        self.sqlite_conn_id = sqlite_conn_id
        self.tables = tables
        
    
    def execute(self, context):
        sqlite_hook = SqliteHook(sqlite_conn_id=self.sqlite_conn_id)
        
        for table in self.tables:
            print(self.tables, table)
            self.log.info("Validating data quality on table: {}".format(table))
            
            records=sqlite_hook.get_records("SELECT COUNT(*) FROM {};".format(table))          
            row_records = records[0][0]
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("Data validation FAILED for table: {}".format(table))
                raise ValueError("Data validation FAILED for table: {}".format(table))
            if (row_records < 1):
                self.log.error("Data validation FAILED for table. Table {} contained 0 rows".format(table))
                raise ValueError("Data validation FAILED for table. Table {} contained 0 rows".format(table))
            self.log.info("Data validation on table {} PASSED".format(table))
        
        