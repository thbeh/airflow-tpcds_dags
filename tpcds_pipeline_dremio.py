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


FILENAME = 'test.csv'
FILEPATH = 'upload'
SFTP_PATH = '/{0}/{1}'.format(FILEPATH, FILENAME)

SSH_CONN_ID = 'my_conn_sftp'
S3_CONN_ID = 'my_conn_s3'
S3_NB_BUCKET = 's3contents-demo'
S3_NB_KEY = 'notebooks'

S3_PROJECT_BUCKET = 'source-data'
S3_PROJECT_KEY = 'tpcds'

today = "{{ ds }}"

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}

dag = DAG(
    'tpcds_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


def check_for_file_py(**kwargs):
    path = kwargs.get('path', None)
    sftp_conn_id = kwargs.get('sftp_conn_id', None)
    ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
    sftp_client = ssh_hook.get_conn().open_sftp()
    ftp_files = sftp_client.listdir(path)

    logging.info(ftp_files)

    if len(ftp_files) == 0:
        return False
    else:
        return True

def upload_file_to_S3(**kwargs):
    bucket_name = kwargs.get('bucket',None)
    key = kwargs.get('key', None)
    path = kwargs.get('path', None)
    sftp_conn_id = kwargs.get('sftp_conn_id', None)
    my_conn_s3 = kwargs.get('s3_conn_id', None)

    s3_hook = S3Hook(my_conn_s3)
    s3_client = s3_hook.get_conn()

    ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
    sftp_client = ssh_hook.get_conn().open_sftp()

    ftp_files = sftp_client.listdir(path)
    for f in ftp_files:
        filename = '{0}/{1}'.format(path, f)
        sftp_client.get(filename, f)
        s3_client.upload_file(f, bucket_name, '{0}/{1}'.format(key, f))

def source_to_persist(**kwargs):
    ts = kwargs.get('ts', None)
    bucket = kwargs.get('bucket', None)
    key = kwargs.get('key', None)
    source_nb = kwargs.get('source_nb', None)
    my_conn_s3 = kwargs.get('s3_conn_id', None)

    s3_hook = S3Hook(my_conn_s3)
    s3_client = s3_hook.get_conn()

    source_f = '{0}/{1}.ipynb'.format(key, source_nb)
    logging.info('key: ' + str(source_f))

    source_s3 = s3_hook.get_key(source_f, bucket)

    with NamedTemporaryFile("w") as f_source:
        logging.info(
                "Dumping S3 file %s contents to local file %s",
                source_s3, f_source.name
            )
        s3_client.download_file(bucket, source_f, f_source.name)

        pm.execute_notebook(
            f_source.name,
            '/tmp/{0}_out-{1}.ipynb'.format(source_nb, ts),
            parameters={'message': ts})    

    s3_client.upload_file('/tmp/{0}_out-{1}.ipynb'.format(source_nb,ts), bucket, key+'/output/{0}_out-{1}.ipynb'.format(source_nb,ts))

def tpcds_transform(**kwargs):
    ts = kwargs.get('ts', None)
    bucket = kwargs.get('bucket', None)
    key = kwargs.get('key', None)
    source_nb = kwargs.get('source_nb', None)
    my_conn_s3 = kwargs.get('s3_conn_id', None)

    s3_hook = S3Hook(my_conn_s3)
    s3_client = s3_hook.get_conn()

    source_f = '{0}/{1}.ipynb'.format(key, source_nb)
    logging.info('key: ' + str(source_f))

    source_s3 = s3_hook.get_key(source_f, bucket)

    with NamedTemporaryFile("w") as f_source:
        logging.info(
                "Dumping S3 file %s contents to local file %s",
                source_s3, f_source.name
            )
        s3_client.download_file(bucket, source_f, f_source.name)

        pm.execute_notebook(
            f_source.name,
            '/tmp/{0}_out-{1}.ipynb'.format(source_nb, ts),
            parameters={'message': ts })    

    s3_client.upload_file('/tmp/{0}_out-{1}.ipynb'.format(source_nb,ts), bucket, key+'/output/{0}_out-{1}.ipynb'.format(source_nb,ts))

with dag:

    start = DummyOperator(task_id='start_task')

    files = ShortCircuitOperator(task_id='check_for_files',
                python_callable=check_for_file_py,
                op_kwargs={"path": FILEPATH,
                           "sftp_conn_id": SSH_CONN_ID},
                provide_context=True)

    extract = PythonOperator(task_id='extract_file_to_S3',
                python_callable=upload_file_to_S3,
                op_kwargs={
                           'path': FILEPATH,
                           'sftp_conn_id': SSH_CONN_ID,
                           'key': S3_PROJECT_KEY,
                           'bucket': S3_PROJECT_BUCKET,
                           's3_conn_id': S3_CONN_ID},
                provide_context=True)

    load = PythonOperator(task_id='load_to_persist',
                python_callable=source_to_persist,
                provide_context=True,
                op_kwargs={
                          'ts': '{{ execution_date }}',
                          'key': S3_NB_KEY,
                          'bucket': S3_NB_BUCKET,
                          'source_nb': 'source_to_persist',
                          's3_conn_id': S3_CONN_ID})
    
    transform = PythonOperator(
                task_id='tpcds_transform',
                python_callable=tpcds_transform,
                provide_context=True,
                op_kwargs={
                          'ts': '{{ execution_date }}',
                          'key': S3_NB_KEY,
                          'bucket': S3_NB_BUCKET,
                          'source_nb': 'tpcds_total_sales',
                          's3_conn_id': S3_CONN_ID})

    end = DummyOperator(task_id='end_task')

    start >> files >> extract >> load >> transform >> end 

