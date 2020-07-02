import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

# sys.path.append('/usr/local/airflow/dags/efs')
# from scripts.callables import scrape_file, load_file
# from uw211dashboard.scripts.callables import scrape_file, load_file


'''
Manual Refresh Dag

Refreshes data that could potentially change past records:
A. COVID-19 data
B. Unemployment claims data (mo)

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
    'catchup': False
}

dag = DAG(
    dag_id='211dash_manual_refresh',
    schedule_interval='@once',
    template_searchpath=SEARCH_PATH,
    default_args=args
)

''' Define manual update operators. '''

update_refresh_timestamps = PostgresOperator(
    task_id='update_refresh_timestamps', 
    sql='setLstSccssflRnDt_4refrsh.sql', 
    dag=dag)

update_refresh_timestamps
