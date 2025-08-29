from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

#database and analysis libraries
import teradatasql
from teradataml import create_context, fastload
from sqlalchemy import create_engine
import pyodbc
import psycopg2
import sqlalchemy as sal
import pandas as pd
import datetime as dt

#misc
import urllib
import re
import time
from pendulum import datetime, duration
from curses import echo
from datetime import datetime
from itertools import accumulate

#ca toolbox
from careallies_toolbox import CareAlliesTools as ct

#import process parameters from Variable list 
dag_config = Variable.get("adhoc_etl_variables_list", deserialize_json = True)
skip_days = dag_config['skip_days']
task_retries = dag_config['task_retries']
task_retry_dur = dag_config['task_retry_dur_min']
batch_size = dag_config['batch_size']
run_hour = dag_config['run_hour_EST']+5 #adjustment for UST to EST (+5 hrs)
td_table_name = dag_config['td_table_name']
td_db_name = dag_config['td_db_name']
rs_table_name = dag_config['rs_table_name']
rs_to_td_skip = dag_config['rs_to_td_skip']
td_to_rs_skip = dag_config['td_to_rs_skip']

#DAG Email notification using Airflow
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 30),    
    'retries' : task_retries,
    'retry_delay' : duration(minutes=task_retry_dur),
    'email': ['Bassel.Dakhlallah@Cigna.com'],
    'email_on_failure': True    
    }

# TD global variables
global_conn_id = 'OSS'
global_conn = BaseHook.get_connection(global_conn_id)
global_user = global_conn.login
global_passwd = global_conn.password
global_logmech = 'ldap'
global_host = global_conn.host

def teradata_to_redshift():
    
    if td_to_rs_skip:
        raise AirflowSkipException

    #Step 1: Build the df from Teradata
    td_engine = sal.create_engine('teradatasql://'+ global_user +':' + global_passwd + '@'+ global_host + '/?logmech=ldap&encryptdata=true',echo=True)

    #conn = td_engine.connect()    

    query = f"SELECT * FROM {td_db_name}.{td_table_name}" # WHERE ManagingEntity = 'DALLAS IPA'
    
    df = pd.read_sql(query, td_engine)
    print(f'Table has {df.shape[0]} rows and {df.shape[1]} columns')
           
    #Step 2 : Upload results to Arcadia Foundry v2 (AWS RedShift)
    conn_id_RS = 'Arcadia_v2'
    conn_RS = BaseHook.get_connection(conn_id_RS)
    user_RS = conn_RS.login
    passwd_RS = conn_RS.password
    host_RS = conn_RS.host
    port_RS = str(conn_RS.port)
    database_RS = 'cigna_reporting_prd62'  #v2
    rs_database_schema = 'sandbox'

    engine = create_engine(f'postgresql+psycopg2://{user_RS}:{passwd_RS}@{host_RS}:{port_RS}/{database_RS}')
    conn_RS = engine.connect()        

    conn_RS.execute(f'DROP TABLE IF EXISTS {rs_database_schema}.{rs_table_name}')

    df.to_sql(name=rs_table_name, con=conn_RS, index=False, schema=rs_database_schema, if_exists='replace', method='multi', chunksize=5000)
    
    return

#DAG Scheduling Settings
dag = DAG('adhoc_table_etl'
          , description='A useful DAG for transferring data across servers on an adhoc basis'
          , default_args=default_args
          , schedule_interval='0 0 1 1 *' #At 00:00 on day-of-month 1 in January since it's an adhoc DAG
          , start_date=datetime(2022, 2, 6)
          , catchup=False
          ,tags=['Adhoc','CareAllies'])

teradata_to_redshift = PythonOperator(task_id='teradata_to_redshift', python_callable=teradata_to_redshift, dag=dag)