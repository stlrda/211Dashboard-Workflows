import os, sys, json
from datetime import datetime, timedelta
from airflow.models import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# add parent folder
sys.path.append('.')
from scripts.callables import scrape_file, load_file

'''
covid_daily dag runs the various Python and Postgres Operator tasks
to bring in data daily and to eventually move data to core tables
More to come...

'''

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 4),  # change this
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='daily_covid_to_staging',
    schedule_interval='@daily',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/',
    default_args=args
)

#####################################################################
# define covid_county_full operators and variables

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
# define covid_zip_stl_county operators and variables

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
# define covid_zip_stl_city operators and variables

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

chain(
    [scrape_covid_county_full, scrape_covid_zip_stl_city, scrape_covid_zip_stl_county],
    [load_covid_county_full_staging, load_covid_zip_stl_city_staging, load_covid_zip_stl_county_staging]
)