import os, sys
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.helpers import chain

sys.path.append('.')
from scripts.callables import scrape_file, load_file, scrape_api
# from dags.211dashboard.scripts.callables import scrape_file, load_file, scrape_api


'''
Startup Configuration DAG

1. Create Airflow "connections" (only for development environment)
2. Create tables
    a. Staging, Lookup, Core, and Schedule tables
    b. Schedule table is designed to record the datetime of last successful dag runs
       This may be used in future dags for data pull coordinations
3. Backfill unemployment data (core or staging?)
    a. 2019 Unemployment stats data from BLS
    b. Full MO Unemployment claims dataset from data.mo.gov API
4. Populate "static" core tables
    a. Census and Funding data (will also be included in "manual" dag)
    b. Lookup tables - ideally won't change over the course of the project

'''

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
SEARCH_PATH = f'{AIRFLOW_HOME}/scripts/sql/'  # development
# SEARCH_PATH = f'{AIRFLOW_HOME}/dags/211dashboard/scripts/sql/'  # production

args = {
    'owner': '211dashboard',
    'start_date': datetime(2020, 5, 25),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    'catchup': False,
}

dag = DAG(
    dag_id='211dash_startup',
    schedule_interval='@once',
    template_searchpath=SEARCH_PATH,
    default_args=args
)

create_staging_unemployment_claims_211 = PostgresOperator(
    task_id='create_staging_unemployment_claims_211', 
    sql='crTbl_stgMoNmplymntClmsAnd211Dta.sql', 
    dag=dag)

create_staging_bls_unemployment_data = PostgresOperator(
    task_id='create_staging_bls_unemployment_data', 
    sql='crTbl_stgBLS_unemplDta.sql', 
    dag=dag)

create_staging_covid_zip = PostgresOperator(
    task_id='create_staging_covid_zip', 
    sql='crTbl_stgCovidUpdtZpCtyAndCnty.sql', 
    dag=dag)

create_staging_covid_full = PostgresOperator(
    task_id='create_staging_covid_full', 
    sql='crTbl_stgCovidDlyVizByCntyAll.sql', 
    dag=dag)

# create_lookup_interest_areas = PostgresOperator(
#     task_id='create_lookup_interest_areas', 
#     sql='crTbl_lkupZpCdAndAreasOfInterest.sql', 
#     dag=dag)

create_static_regional_funding = PostgresOperator(
    task_id='create_static_regional_funding', 
    sql='crTbl_creStlRgnlFndngClnd.sql', 
    dag=dag)

create_success_date_covid_unemployment_core_tables = PostgresOperator(
    task_id='create_success_date_covid_unemployment_core_tables', 
    sql='crTbl_creRstOthrs.sql', 
    dag=dag)

# create_lookup_zip_tract_geo = PostgresOperator(
#     task_id='create_lookup_zip_tract_geo', 
#     sql='crTbl_lkupZipTractGeo.sql', 
#     dag=dag)

create_core_census = PostgresOperator(
    task_id='create_core_census', 
    sql='crTbl_creCnssDta.sql', 
    dag=dag)

create_core_census_views = PostgresOperator(
    task_id='create_core_census_views', 
    sql='crVu_creVuCnssDta.sql', 
    dag=dag)

grant_read_permissions = PostgresOperator(
    task_id='grant_read_permissions', 
    sql='usr_dataviz_grnts.sql', 
    dag=dag)

create_dataviz_functions = PostgresOperator(
    task_id='create_dataviz_functions', 
    sql='crFnctns_dataviz.sql', 
    dag=dag)

#TODO decide if we are keeping this (any use for it in manual dag???)
# load_areas_of_interest = PythonOperator(
#     task_id='load_areas_of_interest',
#     python_callable=load_file,
#     op_kwargs={
#         'filename': 'areasOfNtrst_geoScope_pipDlm.csv',
#         'table_name': 'lkup_areas_of_intr_geo_scope',
#         'sep': '|',
#         'nullstr': ''
#     },
#     dag=dag)

# load_zip_tract_geo = PythonOperator(
#     task_id='load_zip_tract_geo',
#     python_callable=load_file,
#     op_kwargs={
#         'filename': 'zip_tract_geoid.csv',
#         'table_name': 'lkup_zip_tract_geoid',
#         'sep': '|',
#         'nullstr': ''
#     },
#     dag=dag)

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

''' 
Define operators for "backfill" of 
"static" unemployment data from BLS

Incremental data loads will continue monthly
(see monthly DAG)

'''
load_bls_unemployment_2019_staging = PythonOperator(
    task_id='load_bls_unemployment_2019_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': 'unemployment_stats_2019.csv',
        'table_name': 'stg_bls_unemployment_data_2019',
        'sep': '|',
        'nullstr': ''
    },
    dag=dag)

unemployment_2019_data_staging_to_core = PostgresOperator(
    task_id='unemployment_2019_data_staging_to_core', 
    sql='dtaMgrtn_unemplStats_strtup.sql', 
    dag=dag)

update_monthly_timestamp_startup = PostgresOperator(
    task_id='update_monthly_timestamp_startup', 
    sql='setLstSccssflRnDt_mnthlyAll.sql', 
    dag=dag) 

''' 
Define operators for "backfill" of older 
unemployment claims data from data.mo.gov

Incremental data loads will continue weekly via API
(see weekly DAG)

'''
scrape_unemployment_claims_full = PythonOperator(
    task_id='scrape_unemployment_claims_full',
    python_callable=scrape_file,
    op_kwargs={
        'url': 'https://data.mo.gov/api/views/qet9-8yam/rows.csv?accessType=DOWNLOAD',
        'filename': 'mo_unemployment_claims.csv',
        'filters': None,
        'nullstr': ''
    },
    dag=dag)

load_unemployment_claims_full_staging = PythonOperator(
    task_id='load_unemployment_claims_full_staging',
    python_callable=load_file,
    op_kwargs={
        'filename': 'mo_unemployment_claims.csv',
        'table_name': 'stg_mo_unemployment_clms',
        'sep': ',',
        'nullstr': ''
    },
    dag=dag)

startup_unemployment_claims_staging_to_core = PostgresOperator(
    task_id='startup_unemployment_claims_staging_to_core', 
    sql='dtaMgrtn_unemplClms_wkly.sql', 
    dag=dag) 

update_weekly_timestamp_startup = PostgresOperator(
    task_id='update_weekly_timestamp_startup', 
    sql='setLstSccssflRnDt_wklyAll.sql', 
    dag=dag) 


# Utilize "chain" function for more complex relationships among dag operators
chain(
    [create_staging_unemployment_claims_211,  # create tables
        create_staging_bls_unemployment_data,
        create_staging_covid_zip,
        create_staging_covid_full,
        create_static_regional_funding,
        create_success_date_covid_unemployment_core_tables,
        #create_lookup_zip_tract_geo,
        create_core_census,
        create_core_census_views],
    grant_read_permissions,
    create_dataviz_functions,
    [#load_zip_tract_geo,  # load "static" data tables
        load_static_regional_funding,
        load_census_by_county,
        load_census_by_tract],
    scrape_unemployment_claims_full,  # begin loading of all unemployment data
    [load_bls_unemployment_2019_staging,
        load_unemployment_claims_full_staging],
    [unemployment_2019_data_staging_to_core,
        startup_unemployment_claims_staging_to_core],
    [update_monthly_timestamp_startup,
        update_weekly_timestamp_startup]
    #TODO
    # create_lookup_interest_areas
    # load_areas_of_interest
)