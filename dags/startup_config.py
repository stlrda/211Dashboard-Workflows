import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

sys.path.append('.')
from scripts.create_connections import create_connections
#from scripts.callables import scrape_file, load_file, scrape_api


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
    a. Census and Funding data

'''

#NOTE: May not need this variable for production environment.
# AIRFLOW_HOME required for running scripts with Docker config.
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 4),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='startup',
    schedule_interval='@once',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/',
    default_args=args
)

'''
Installing requirements through BashOperator does not produce desired results
when running airflow via docker.
Best practice would be to install these requirements from requirements.txt file
upon infrastructure initiation.
#NOTE may modify this config.
'''
#install_requirements = BashOperator(task_id='install_requirements', bash_command='pip install -r $AIRFLOW_HOME/requirements.txt', dag=dag)

def WhereAmI():
    print('This function was ran from ' + str(os.getcwd()))

#TODO remove "create_connections" operator for production environment
# This operator should already exist in the Airflow Admin repository
create_connections = PythonOperator(task_id='create_connections', python_callable=create_connections, dag=dag)

#TODO add other startup configurations
#scripts/crTbl_creRstOthrs.sql

create_staging_unemployment_211 = PostgresOperator(task_id='create_staging_unemployment_211', sql='crTbl_stgMoNmplymntClmsAnd211Dta.sql', dag=dag) 
create_staging_covid_zip = PostgresOperator(task_id='create_staging_covid_zip', sql='crTbl_stgCovidUpdtZpCtyAndCnty.sql', dag=dag) 
create_staging_covid_full = PostgresOperator(task_id='create_staging_covid_full', sql='crTbl_stgCovidDlyVizByCntyAll.sql', dag=dag)
create_lookup_tables = PostgresOperator(task_id='create_lookup_tables', sql='crTbl_lkupZpCdAndAreasOfInterest.sql', dag=dag)
create_static_regional_funding = PostgresOperator(task_id='create_static_regional_funding', sql='crTbl_creStlRgnlFndngClnd.sql', dag=dag)
create_success_date_and_covid_core = PostgresOperator(task_id='create_success_date_and_covid_core', sql='crTbl_creRstOthrs.sql', dag=dag)

# Utilize "chain" function for more complex relationships among dag operators
create_connections >> [create_staging_unemployment_211,
                       create_staging_covid_zip,
                       create_staging_covid_full,
                       create_lookup_tables,
                       create_static_regional_funding,
                       create_success_date_and_covid_core]