'''
=================================================
Milestone 3

Nama  : Ahmad Darmawan
Batch : CODA-011-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL
ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai produktifitas karyawan.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Default configuration for the entire DAG
default_args = {
    'owner': 'aan',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# Define DAG
with DAG(
    'rmt11_employee',
    default_args=default_args,
    schedule_interval='10-30 9 * * 6',
    catchup=False,
) as dag:

    python_extract = BashOperator(task_id='python_extract', bash_command='python /opt/airflow/scripts/rmt011_extract.py')
    python_transform = BashOperator(task_id='python_transform', bash_command='python /opt/airflow/scripts/rmt011_transform.py')
    python_load = BashOperator( task_id='python_load', bash_command='python /opt/airflow/scripts/rmt011_load.py')

# Mengatur urutan workflow (Extract -> Transform -> Load)
python_extract >> python_transform >> python_load