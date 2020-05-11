import os, sys, json
from datetime import datetime, timedelta
from airflow.models import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.baseoperator import chain

# add parent folder
sys.path.append('.')
from scripts.callables import scrape_file, load_file

'''
Daily DAG

1. Scrape COVID-19 data
    a. Data sourced from Chris Prener's github at
       "https://github.com/slu-openGIS/covid_daily_viz/tree/master/data"
2. Truncate staging tables
3. Load COVID-19 data to staging tables
4. Load 211 data to staging tables
    a. NOTE this data will eventually have to be scraped (ideally daily)
       however for now we will just use a static file in the s3 bucket
       "sample_211_mo_data_ ... .csv"
5. Move data from staging to core tables
    a. Apply appropriate filters and aggregations
6. Update timestamp

'''

#NOTE: May not need this variable for production environment.
# AIRFLOW_HOME required for running scripts with Docker config.
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 10),  # change this
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='daily',
    schedule_interval='@daily',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/',
    default_args=args
)

#####################################################################
''' Define covid_county_full variables and operators. '''

covid_county_full_url = 'https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/county/county_full.csv'
covid_county_full_file = 'covid_county_full.csv'
covid_county_full_table = 'stg_covid_dly_viz_cnty_all'

scrape_covid_county_full = PythonOperator(
    task_id='scrape_covid_county_full',
    python_callable=scrape_file,
    op_kwargs={
        'url': covid_county_full_url,
        'filename': covid_county_full_file,
        'filters': {'state': ['Missouri', 'Illinois']},
        'nullstr': 'NaN'
    },
    dag=dag)

load_covid_county_full_staging = PythonOperator(
    task_id='load_covid_county_full_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': covid_county_full_file,
        'table_name': covid_county_full_table,
        'sep': ',',
        'nullstr': 'NaN'
    },
    dag=dag)

#####################################################################
''' Define covid_zip_stl_county variables and operators. '''

covid_zip_stl_county_url = 'https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/zip/zip_stl_county.csv'
covid_zip_stl_county_file = 'covid_zip_stl_county.csv'
covid_zip_stl_county_table = 'stg_covid_zip_stl_county'

scrape_covid_zip_stl_county = PythonOperator(
    task_id='scrape_covid_zip_stl_county',
    python_callable=scrape_file,
    op_kwargs={
        'url': covid_zip_stl_county_url,
        'filename': covid_zip_stl_county_file,
        'filters': None,
        'nullstr': 'NaN'
    },
    dag=dag)

load_covid_zip_stl_county_staging = PythonOperator(
    task_id='load_covid_zip_stl_county_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': covid_zip_stl_county_file,
        'table_name': covid_zip_stl_county_table,
        'sep': ',',
        'nullstr': 'NaN'
    },
    dag=dag)

#####################################################################
''' Define covid_zip_stl_city variables and operators. '''

covid_zip_stl_city_url = 'https://raw.githubusercontent.com/slu-openGIS/covid_daily_viz/master/data/zip/zip_stl_city.csv'
covid_zip_stl_city_file = 'covid_zip_stl_city.csv'
covid_zip_stl_city_table = 'stg_covid_zip_stl_city'

scrape_covid_zip_stl_city = PythonOperator(
    task_id='scrape_covid_zip_stl_city',
    python_callable=scrape_file,
    op_kwargs={
        'url': covid_zip_stl_city_url,
        'filename': covid_zip_stl_city_file,
        'filters': None,
        'nullstr': 'NaN'
    },
    dag=dag)

load_covid_zip_stl_city_staging = PythonOperator(
    task_id='load_covid_zip_stl_city_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': covid_zip_stl_city_file,
        'table_name': covid_zip_stl_city_table,
        'sep': ',',
        'nullstr': 'NaN'
    },
    dag=dag)


#####################################################################
''' Truncate daily staging tables for loading. '''

truncate_daily_staging_tables = PostgresOperator(
    task_id='truncate_daily_staging_tables', 
    sql='trnctTbls_dly.sql', 
    dag=dag) 


#####################################################################
''' Load 211 daily staging tables from S3
    filename: "sample_211_mo_data.csv" 

    #NOTE this step will likely change once UnitedWay configures their
    211 data API.

'''

load_211_staging = PythonOperator(
    task_id='load_211_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': 'sample_211_mo_data.csv',
        'table_name': 'stg_mo_211_data',
        'sep': ',',
        'nullstr': 'NULL'
    },
    dag=dag)


#####################################################################
''' Move COVID-19 data from staging tables to core table. '''

covid_staging_to_core = PostgresOperator(
    task_id='covid_staging_to_core', 
    sql='cvdDtaMgrtn_dly.sql', 
    dag=dag) 


#####################################################################
''' Move 211 data from staging to core table. '''

211_staging_to_core = PostgresOperator(
    task_id='211_staging_to_core', 
    sql='211DtaMgrtn_dly.sql', #TODO
    dag=dag) 


#####################################################################
''' Updates "DLY_ALL" key in "cre_last_success_run_dt" upon 
    successful execution of daily dag '''

update_daily_timestamp = PostgresOperator(
    task_id='update_daily_timestamp', 
    sql='setLstSccssflRnDt_dlyAll.sql',
    dag=dag) 


#####################################################################
''' Set relationships among Operators in Daily DAG. '''

chain(
    [scrape_covid_county_full, scrape_covid_zip_stl_city, scrape_covid_zip_stl_county],
    truncate_daily_staging_tables,
    [load_covid_county_full_staging, load_covid_zip_stl_city_staging, load_covid_zip_stl_county_staging],
    load_211_staging,
    covid_staging_to_core, #TODO remove this once 211 staging-->core is ready
    #[covid_staging_to_core, 211_staging_to_core],
    update_daily_timestamp
)