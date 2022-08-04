from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#   Function to ingest data
def ingest_data():
    s3_hook = S3Hook(aws_conn_id = 'aws_default')
    psql_hook = PostgresHook(postgres_conn_id = 'rds_connection')
    file = s3_hook.dowload_file(
        key = 'raw_data/user_purchase.csv', bucket_name = 's3-data-bootcamp'
    )
    psql_hook.bulk_load(table = 'purchase_raw.user_purchase', tmp_file = file) 

with DAG(
    'db_ingestion', start_date = days_ago(1), schedule_interval = '@once'
) as dag:
    start_workflow = DummyOperator(task_id = 'start_workflow')
    validate = S3KeySensor(
        task_id = 'validate',
        aws_conn_id = 'aws_default',
        bucket_name = 's3-data-bootcamp',
        bucket_key = 'raw_data/user_purchase.csv',
    )
    prepare = PostgresOperator(
        task_id = 'prepare',
        postgres_conn_id = 'rds_connection',
        sql = '''
            CREATE SCHEMA IF NOT EXISTS purchase_raw;
            CREATE TABLE IF NOT EXIST purchase_raw.user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            );
        ''',
    )

    clear = PostgresOperator(
        task_id = 'clear',
        postgres_conn_id = 'rds_connection',
        sql = '''DELETE FROM purchase_raw.user_purchase''',
    )

    continue_workflow = DummyOperator(task_id = 'continue_workflow')
    
    branch = BranchSQLOperator(
        task_id = 'is_empty',
        conn_id = 'rds_connection',
        sql = 'SELECT COUNT(*) AS rows FROM purchase_raw.user_purchase',
        follow_task_ids_if_true = [clear.task_id],  # >=1
        follow_task_ids_if_fals = [continue_workflow.task_id], # ==0
    )
    load = PythonOperator(
        task_id = 'load',
        python_callable = ingest_data,
        trigger_rule = TriggerRule.ONE_SUCCESS,
    )
    end_workflow = DummyOperator(task_id = 'end_workflow')

    start_workflow >> validate >> prepare >> branch
    branch >> [clear, continue_workflow] >> load >> end_workflow