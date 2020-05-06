import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

sys.path.append(".")
from scripts.create_connections import create_connections

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
    dag_id='startup_config',
    schedule_interval='@once',
    template_searchpath=f'{AIRFLOW_HOME}/scripts/',
    default_args=args
)

# install requirements in environment configuration
#install_requirements = BashOperator(task_id='install_requirements', bash_command='pip install -r $AIRFLOW_HOME/requirements.txt', dag=dag)

# only for local testing purposes
create_connections = PythonOperator(task_id='create_connections', python_callable=create_connections, dag=dag)

create_staging_unemployment_211 = PostgresOperator(task_id='create_staging_unemployment_211', sql='crTbl_stgMoNmplymntClmsAnd211Dta.sql', dag=dag) 
create_staging_covid_zip = PostgresOperator(task_id='create_staging_covid_zip', sql='crTbl_stgCovidUpdtZpCtyAndCnty.sql', dag=dag) 
create_staging_covid_full = PostgresOperator(task_id='create_staging_covid_full', sql='crTbl_stgCovidDlyVizByCntyAll.sql', dag=dag)
create_lookup_tables = PostgresOperator(task_id='create_lookup_tables', sql='crTbl_lkupZpCdAndAreasOfInterest.sql', dag=dag)
create_static_regional_funding = PostgresOperator(task_id='create_static_regional_funding', sql='crTbl_creStlRgnlFndngClnd.sql', dag=dag)

create_connections >> [create_staging_unemployment_211,
                       create_staging_covid_zip,
                       create_staging_covid_full,
                       create_lookup_tables,
                       create_static_regional_funding]