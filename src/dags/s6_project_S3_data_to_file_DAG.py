import os
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import boto3
from airflow.models import Variable

log = logging.getLogger(__name__)

def get_s3_file(bucket: str, key: str):
    logging.info ('Начало загрузки из S3')
# airflow vars  
    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
    print(AWS_ACCESS_KEY_ID)
    print(AWS_SECRET_ACCESS_KEY)
                                         
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'                                                     # path to container
    )

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project6_dag_get_data():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=get_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]
    fetch_tasks

dag = project6_dag_get_data()