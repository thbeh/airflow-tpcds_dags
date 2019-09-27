from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
import pandas as pd
import papermill as pm

import logging
import json
import boto3
from botocore.client import Config

import airflow
import requests

from airflow import DAG
from airflow.hooks import S3Hook
from airflow.contrib.hooks.ssh_hook import SSHHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.papermill_operator import PapermillOperator

from airflow.contrib.operators.sftp_to_s3_operator import SFTPToS3Operator
from airflow.contrib.operators.sftp_operator import SFTPOperator

SSH_CONN_ID = 'sftp_default'

FILENAME = 'test.csv'
FILEPATH = 'upload'
SFTP_PATH = '/{0}/{1}'.format(FILEPATH, FILENAME)

S3_CONN_ID = 'my_conn_s3'
S3_BUCKET = 's3contents-demo'
S3_KEY = 'notebooks'

today = "{{ ds }}"

s3 = boto3.resource('s3',
                    endpoint_url='http://minio:9000',
                    aws_access_key_id='minio',
                    aws_secret_access_key='minio123',
                    region_name='us-east-1')

S3_KEY_TRANSFORMED = '{0}_{1}.json'.format(S3_KEY, today)

default_args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'tpcds_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

def start_task():
    return 'start_task'

def end_task():
    return 'end_task'

def transform_notebook(**kwargs):
    bucket = kwargs.get('bucket', None)
    key = kwargs.get('key', None)
    nb = kwargs.get('source_nb', None)
    source_nb = '{0}.ipynb'.format(nb)
    output_nb = '/tmp/{0}.ipynb'.format(nb)
    my_conn_s3 = kwargs.get('s3_conn_id', None)

    s3_hook = S3Hook(my_conn_s3)
    s3_client = s3_hook.get_conn()

    source_f = '{0}/{1}'.format(key, source_nb)
    logging.info('key: ' + str(source_f))

    source_s3 = s3_hook.get_key(source_f, bucket)
#   output_s3 = s3_hook.get_key(output_f, bucket)

    with NamedTemporaryFile("w") as f_source:
        logging.info(
                "Dumping S3 file %s contents to local file %s",
                source_s3, f_source.name
            )
        s3_client.download_file(bucket, source_f, f_source.name)

        pm.execute_notebook(
            f_source.name,
            '/tmp/nb_out.ipynb',
            parameters={'message':'{{ execution_date }}'})    

def run_druid_indexing(**kwargs):
    bucket = kwargs.get('bucket', None)
    key = kwargs.get('key', None)
    source_js = kwargs.get('source_js', None)
    my_conn_s3 = kwargs.get('s3_conn_id', None)
    my_http_id = kwargs.get('http_conn_id', None)

    endpoint = kwargs.get('endpoint', None)

    s3_hook = S3Hook(my_conn_s3)
    source_f = '{0}/{1}'.format(key, source_js)

    source_s3 = s3_hook.get_key(source_f, bucket)
    file_content = source_s3.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)

    url = '{}/{}'.format(my_http_id, endpoint)
    headers = {'Content-Type': 'application/json'}

    r = requests.post(url, data=file_content, headers=headers)
    logging.info('response %s', r.text)

with dag:

    start = PythonOperator(
                       task_id='start_task',
                       python_callable=start_task)

    transform = PythonOperator(
                       task_id='run_example_notebook',
                       python_callable=transform_notebook,
                       op_kwargs={
                          'key': S3_KEY,
                          'bucket': S3_BUCKET,
                          'source_nb': 'tpcds_total_sales',
                          's3_conn_id': S3_CONN_ID})

    consumption = PythonOperator(
                       task_id='run_index_druid',
                       python_callable=run_druid_indexing,
                       op_kwargs={
                          'key': 'druid',
                          'bucket': S3_BUCKET,
                          'endpoint': 'druid/indexer/v1/task',
                          's3_conn_id': S3_CONN_ID,
                          'source_js': 's3_tpcds.json',
                          'http_conn_id': 'http://overlord:8090'})

    end = PythonOperator(task_id='end_task',
                       python_callable=end_task)

    start >> transform >> consumption >> end 

