from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

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
    default_args=args
)

#TODO create connections - ask Jonathan
install_requirements = BashOperator(task_id='install_requirements', bash_command="pip install -r requirements.txt")
#create_staging_tables = None
#create_core_tables = None

#install_requirements >> create_staging_tables >> create_core_tables