import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

sys.path.append('.')
from scripts.callables import scrape_file, load_file
#TODO for production environment change module paths -- such as...
#from dags.211Dashboard.scripts.callables import scrape_file, load_file, scrape_api


'''
Manual Update DAG

Designed to update any "static" data tables that have changed.
When triggered...

1. Update Census data
    a. Tract and County (?)
    b. #NOTE must decide if this will be load or if data transformation will occur as well
2. Update Regional Funding data
    a. #NOTE simple load or transformation (?)
3. Other potential updates:
    a. Areas of Interest lookup table
    b. Zip-tract-geoid lookup table (if areas of interest change)

More info to come...

'''

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
#NOTE: AIRFLOW_HOME variable will be different in production environment

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 21),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='manual',
    schedule_interval='@once',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/sql/', #TODO production_path = AIRFLOW_HOME/dags/211dashboard/scripts/
    default_args=args
)

''' Define manual update operators. '''

trigger_manual_test = BashOperator(
    task_id='trigger_manual_test',
    bash_command='echo Triggered manual dag!',
    dag=dag,
)

trigger_manual_test