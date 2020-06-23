import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

sys.path.append('.')
from scripts.callables import transform_static_s3, load_file
from scripts.s3_transformers import transform_census_data, transform_funding_data
from scripts.s3_transformers import transform_crosswalk_files, generate_areas_of_interest
# from dags.211dashboard.scripts.callables import transform_static_s3, load_file
# from dags.211dashboard.scripts.s3_transformers import transform_census_data, transform_funding_data
# from dags.211dashboard.scripts.s3_transformers import transform_crosswalk_files, generate_areas_of_interest


'''
Manual Update DAG

Designed to update any "static" data tables that have changed.
When triggered...

1. Update Census data
    a. Tract and County
2. Update Regional Funding data
3. Update HUD Crosswalk files (3 in total)
    a. zip --> county
    b. county --> zip
    c. tract --> zip
4. Update areas of interest (derived from areas_of_interest.json and census data)

'''

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
SEARCH_PATH = f'{AIRFLOW_HOME}/scripts/sql/'  # development
RESOURCE_PATH = f'{AIRFLOW_HOME}/resources/'  # development
# SEARCH_PATH = f'{AIRFLOW_HOME}/dags/211dashboard/scripts/sql/'  # production
# RESOURCE_PATH = f'{AIRFLOW_HOME}/dags/211dashboard/resources/'  # production

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 6, 1),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='211dash_manual_update',
    schedule_interval='@once',
    template_searchpath=SEARCH_PATH,
    default_args=args
)

''' Define manual update operators. '''

''' 1. Census data operators '''

truncate_core_census_tables = PostgresOperator(
    task_id='truncate_core_census_tables', 
    sql='trnctTbls_census.sql', 
    dag=dag) 

transform_census_county_files = PythonOperator(
    task_id='transform_census_county_files',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'census_county',
        'filename': 'census_data_by_county.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': transform_census_data,
        'sep': '|'
    },
    dag=dag)

transform_census_tract_files = PythonOperator(
    task_id='transform_census_tract_files',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'census_tract',
        'filename': 'census_data_by_tract.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': transform_census_data,
        'sep': '|',
    },
    dag=dag)

load_census_by_county = PythonOperator(
    task_id='load_census_by_county',
    python_callable=load_file,
    op_kwargs={
        'filename': 'census_data_by_county.csv',
        'table_name': 'cre_census_data_by_county_yr2018',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

load_census_by_tract = PythonOperator(
    task_id='load_census_by_tract',
    python_callable=load_file,
    op_kwargs={
        'filename': 'census_data_by_tract.csv',
        'table_name': 'cre_census_data_by_tract_yr2018',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

''' 2. Funding data operators '''

truncate_funding_table = PostgresOperator(
    task_id='truncate_funding_table', 
    sql='trnctTbls_funding.sql', 
    dag=dag)

transform_funding_file = PythonOperator(
    task_id='transform_funding_file',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'funding_data_final_csv_public.csv',
        'filename': 'funding_data_final_public_pipedlm.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': transform_funding_data,
        'sep': '|'
    },
    dag=dag)

load_static_regional_funding = PythonOperator(
    task_id='load_static_regional_funding',
    python_callable=load_file,
    op_kwargs={
        'filename': 'funding_data_final_public_pipedlm.csv',
        'table_name': 'cre_stl_rgnl_fndng_clnd',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

''' 3. Crosswalk data operators '''

truncate_crosswalk_tables = PostgresOperator(
    task_id='truncate_crosswalk_tables', 
    sql='trnctTbls_crosswalk.sql', 
    dag=dag)

transform_zip_county_crosswalk = PythonOperator(
    task_id='transform_zip_county_crosswalk',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'ZIP_COUNTY',  # must be capitalized
        'filename': 'zip_county_mapping_gtwy_rgnl.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': transform_crosswalk_files,
        'sep': '|'
    },
    dag=dag)

transform_county_zip_crosswalk = PythonOperator(
    task_id='transform_county_zip_crosswalk',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'COUNTY_ZIP',  # must be capitalized
        'filename': 'county_zip_mapping_gtwy_rgnl.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': transform_crosswalk_files,
        'sep': '|'
    },
    dag=dag)

transform_tract_zip_crosswalk = PythonOperator(
    task_id='transform_tract_zip_crosswalk',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'TRACT_ZIP',  # must be capitalized
        'filename': 'tract_zip_mapping_gtwy_rgnl.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': transform_crosswalk_files,
        'sep': '|'
    },
    dag=dag)

load_static_zip_county_mapping = PythonOperator(
    task_id='load_static_zip_county_mapping',
    python_callable=load_file,
    op_kwargs={
        'filename': 'zip_county_mapping_gtwy_rgnl.csv',
        'table_name': 'lkup_zip_county_mpg_gtwy_rgnl',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

load_static_county_zip_mapping = PythonOperator(
    task_id='load_static_county_zip_mapping',
    python_callable=load_file,
    op_kwargs={
        'filename': 'county_zip_mapping_gtwy_rgnl.csv',
        'table_name': 'lkup_county_zip_mpg_gtwy_rgnl',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

load_static_tract_zip_mapping = PythonOperator(
    task_id='load_static_tract_zip_mapping',
    python_callable=load_file,
    op_kwargs={
        'filename': 'tract_zip_mapping_gtwy_rgnl.csv',
        'table_name': 'lkup_tract_zip_mpg_gtwy_rgnl',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)


''' 4. Areas of interest operators '''

truncate_interest_table = PostgresOperator(
    task_id='truncate_interest_table', 
    sql='trnctTbls_geoIntrsts.sql', 
    dag=dag)

transform_areas_of_interest = PythonOperator(
    task_id='transform_areas_of_interest',
    python_callable=transform_static_s3,
    op_kwargs={
        'data': 'census_county',  # selects counties from file in census_county folder
        'filename': 'areas_of_interest.csv',
        'resource_path': RESOURCE_PATH,
        'transformer': generate_areas_of_interest,
        'sep': '|'
    },
    dag=dag)

load_areas_of_interest = PythonOperator(
    task_id='load_areas_of_interest',
    python_callable=load_file,
    op_kwargs={
        'filename': 'areas_of_interest.csv',
        'table_name': 'lkup_areas_of_intr_geo_scope',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)


chain(
    truncate_core_census_tables,
    [transform_census_county_files, transform_census_tract_files],
    [load_census_by_county, load_census_by_tract],
    truncate_funding_table,
    transform_funding_file,
    load_static_regional_funding,
    truncate_crosswalk_tables,
    [transform_zip_county_crosswalk,
        transform_county_zip_crosswalk,
        transform_tract_zip_crosswalk],
    [load_static_zip_county_mapping,
        load_static_county_zip_mapping,
        load_static_tract_zip_mapping],
    truncate_interest_table,
    transform_areas_of_interest,
    load_areas_of_interest
)