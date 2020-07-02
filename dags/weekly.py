import os, sys, json
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

# add parent folder
sys.path.append('/usr/local/airflow/dags/efs')
# from scripts.callables import scrape_api, load_file
from uw211dashboard.scripts.callables import load_file, scrape_api

'''
Weekly DAG

1. Scrape MO unemployment claims data (from API)
2. Truncate MO unemployment claims staging table
3. Load MO unemployment claims data to staging
4. Move unemployment claims data from staging to core table
    a. "Filtered" by date (see SQL script)
5. Update weekly run success timestamp

'''

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
# SEARCH_PATH = f'{AIRFLOW_HOME}/scripts/sql/'  # development
SEARCH_PATH = f'{AIRFLOW_HOME}/dags/efs/uw211dashboard/scripts/sql/'  # production

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 6, 1),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False #,
    #'email': ['keenan.berry@daugherty.com'], #TODO configure email option
    #'email_on_failure': True
}

dag = DAG(
    dag_id='211dash_weekly',
    schedule_interval='@weekly',
    template_searchpath=SEARCH_PATH,
    default_args=args
)

''' Define weekly airflow operators. '''

scrape_mo_unemployment_claims = PythonOperator(
    task_id='scrape_mo_unemployment_claims',
    python_callable=scrape_api,
    op_kwargs={
        'url': 'data.mo.gov',
        'filename': 'mo_unemployment_claims.csv',
        'table_name': 'qet9-8yam',
        'limit': 2000  # buffer in case airflow cluster goes down for extended time
    },
    dag=dag)

truncate_weekly_staging_tables = PostgresOperator(
    task_id='truncate_weekly_staging_tables', 
    sql='trnctTbls_wkly.sql', 
    dag=dag) 
    
load_mo_unemployment_claims_staging = PythonOperator(
    task_id='load_mo_unemployment_claims_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': 'mo_unemployment_claims.csv',
        'table_name': 'stg_mo_unemployment_clms',
        'sep': ',',
        'nullstr': ''
    },
    dag=dag)

wkly_unemployment_claims_staging_to_core = PostgresOperator(
    task_id='wkly_unemployment_claims_staging_to_core', 
    sql='dtaMgrtn_unemplClms_wkly.sql', 
    dag=dag) 

update_weekly_timestamp = PostgresOperator(
    task_id='update_weekly_timestamp', 
    sql='setLstSccssflRnDt_wklyAll.sql', 
    dag=dag) 

chain(scrape_mo_unemployment_claims,
      truncate_weekly_staging_tables,
      load_mo_unemployment_claims_staging,
      wkly_unemployment_claims_staging_to_core,
      update_weekly_timestamp
)
