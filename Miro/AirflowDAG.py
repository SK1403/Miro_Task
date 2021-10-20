#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Script is use to schedule the PySpark Job using Airflow
#
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 19/10/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
    Input arguments
"""
SRC_DIR = os.getcwd() + '/src/'
DAG_ID = "daily_miro_etl_job_scheduler"
DEFAULT_ARGS  = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 12),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
  }

"""
    Init DAG object
"""
dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Miro_Temp ETL job launch from Airflow',
    dagrun_timeout=timedelta(minutes=60),
    schedule_interval=datetime.timedelta(days=1)
)

t1 = BashOperator(
    task_id='daily catalogue crawler',
    bash_command='python /home/airflow/airflow/dags/scripts/miro_elt_entrypoint.py',
    dag=dag)
