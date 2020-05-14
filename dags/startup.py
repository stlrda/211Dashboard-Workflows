import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

sys.path.append('.')
from scripts.callables import scrape_file, load_file, scrape_api
#TODO for production environment change module paths -- such as...
#from dags.211Dashboard.scripts.callables import scrape_file, load_file, scrape_api


'''
Startup Configuration DAG

1. Create Airflow "connections" (should only be for development environment)
2. Create tables
    a. Staging, Lookup, Core, and Schedule tables
    b. Schedule table is designed to record the datetime of last successful dag runs
       This may be used in future dags for data pull coordinations
3. Populate older unemployment data (e.g. 2019)
    a. For both MO and IL
4. Populate "static" core tables
    a. Census and Funding data (will also be included in "manual" dag)
    b. Lookup tables - ideally won't change over the course of the project

'''

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
#NOTE: AIRFLOW_HOME variable will be different in production environment

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 12),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='startup',
    schedule_interval='@once',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/', #TODO production_path = AIRFLOW_HOME/dags/211dashboard/scripts/
    default_args=args
)

create_staging_unemployment_211 = PostgresOperator(task_id='create_staging_unemployment_211', sql='crTbl_stgMoNmplymntClmsAnd211Dta.sql', dag=dag) 
create_staging_covid_zip = PostgresOperator(task_id='create_staging_covid_zip', sql='crTbl_stgCovidUpdtZpCtyAndCnty.sql', dag=dag) 
create_staging_covid_full = PostgresOperator(task_id='create_staging_covid_full', sql='crTbl_stgCovidDlyVizByCntyAll.sql', dag=dag)
#TODO create file with areas of interest for loading on startup (put file in s3)
create_lookup_interest_areas = PostgresOperator(task_id='create_lookup_interest_areas', sql='crTbl_lkupZpCdAndAreasOfInterest.sql', dag=dag)
create_static_regional_funding = PostgresOperator(task_id='create_static_regional_funding', sql='crTbl_creStlRgnlFndngClnd.sql', dag=dag)
create_success_date_and_covid_core = PostgresOperator(task_id='create_success_date_and_covid_core', sql='crTbl_creRstOthrs.sql', dag=dag)
create_lookup_zip_tract_geo = PostgresOperator(task_id='create_lookup_zip_tract_geo', sql='crTbl_lkupZipTractGeo.sql', dag=dag)
load_areas_of_interest = PythonOperator(
    task_id='load_areas_of_interest',
    python_callable=load_file,
    op_kwargs={
        'filename': 'areasOfNtrst_geoScope_pipDlm.csv',
        'table_name': 'lkup_areas_of_intr_geo_scope',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)
load_zip_tract_geo = PythonOperator(
    task_id='load_zip_tract_geo',
    python_callable=load_file,
    op_kwargs={
        'filename': 'zip_tract_geoid.csv',
        'table_name': 'lkup_zip_tract_geoid',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)
load_static_regional_funding = PythonOperator(
    task_id='load_static_regional_funding',
    python_callable=load_file,
    op_kwargs={
        'filename': 'stl_regional_funding_cleaned.csv',
        'table_name': 'cre_stl_rgnl_fndng_clnd',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)


# Utilize "chain" function for more complex relationships among dag operators
chain(
    create_staging_unemployment_211,
    [create_staging_covid_zip,create_staging_covid_full],
    create_lookup_interest_areas,
    create_static_regional_funding,
    create_success_date_and_covid_core,
    create_lookup_zip_tract_geo,
    load_areas_of_interest,
    load_zip_tract_geo,
    load_static_regional_funding
)