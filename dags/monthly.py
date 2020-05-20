import os, sys, json
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

# add parent folder
sys.path.append('.')
from scripts.callables import scrape_transform, load_file
from scripts.transformers import transform_unemployment_stats
#TODO for production environment change module paths -- such as...
#from dags.211Dashboard.scripts.callables import scrape_file, load_file, scrape_api

'''
Monthly DAG

1. Scrape unemployment statistics from the Bureau of Labor and Statistics
2. Truncate monthly stats staging table
3. Load current unemployment data to staging
4. Transfer data from staging to core
    a. "Filtered" by date (see SQL script)
5. Update monthly run success timestamp

'''

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
#NOTE: AIRFLOW_HOME variable will be different in production environment

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 21),  # change this
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='monthly',
    schedule_interval='@monthly',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/sql/', #TODO production_path = AIRFLOW_HOME/dags/211dashboard/scripts/
    default_args=args
)

''' Define monthly airflow operators. '''

# file @ url contains latest 14 months of data
scrape_unemployment_stats = PythonOperator(
    task_id='scrape_unemployment_stats',
    python_callable=scrape_transform,
    op_kwargs={
        'url': 'https://www.bls.gov/web/metro/laucntycur14.txt',
        'filename': 'unemployment_stats.csv',
        'transformer': transform_unemployment_stats,
        'sep': '|'
    },
    dag=dag)

truncate_monthly_staging_table = PostgresOperator(
    task_id='truncate_monthly_staging_table', 
    sql='trnctTbls_mnthly.sql', 
    dag=dag) 
    
load_current_unemployment_stats_staging = PythonOperator(
    task_id='load_current_unemployment_stats_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': 'unemployment_stats.csv',
        'table_name': 'stg_bls_unemployment_data_curr',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

unemployment_stats_staging_to_core = PostgresOperator(
    task_id='unemployment_stats_staging_to_core', 
    sql='dtaMgrtn_unemplStats_mnthly.sql', 
    dag=dag)

update_monthly_timestamp = PostgresOperator(
    task_id='update_monthly_timestamp', 
    sql='setLstSccssflRnDt_mnthlyAll.sql', 
    dag=dag) 

chain(scrape_unemployment_stats, 
      truncate_monthly_staging_table, 
      load_current_unemployment_stats_staging, 
      unemployment_stats_staging_to_core,
      update_monthly_timestamp
)