import os
import logging
import vertica_python
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

# airflow vars  
conn_info = {
    'host': Variable.get('VERTICA_HOST'),
    'port': Variable.get('VERTICA_PORT'),
    'user': Variable.get('VERTICA_USER'),
    'password': Variable.get('VERTICA_PASSWORD'),
    'database': Variable.get('VERTICA_DATABASE'),
    'autocommit': True
}

def execute_vertica(conn_info=conn_info):
  with vertica_python.connect(**conn_info) as conn:
    cur = conn.cursor()
    logging.info("Коннекшн пошёл")
    cur.execute("DROP TABLE IF EXISTS STV2023081241__STAGING.group_log;")
    cur.execute("DROP TABLE IF EXISTS STV2023081241__STAGING.group_log_rej;")
    cur.execute("""
                    CREATE TABLE STV2023081241__STAGING.group_log
                    (
                        group_id INT NOT NULL,
                        user_id INT,
                        user_id_from INT,
                        event varchar(50),
                        datetime timestamp
                    );
        """)
    cur.execute("""
                    COPY STV2023081241__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
                    FROM LOCAL '/data/group_log.csv'
                    DELIMITER ','
                    REJECTED DATA AS TABLE STV2023081241__STAGING.group_log_rej;
    """)       

    sql = "select count(*) from STV2023081241__STAGING.group_log"
    cur.execute(sql)
    result = cur.fetchall()[0][0]
    logging.info(result)
    logging.info(f'fetched result in DB - {result}')
    cur.execute('COMMIT;')
    cur.close()


default_args = {
    'owner': 'Airflow',
    'schedule_interval':'@once',          
    'retries': 1,                          
    'retry_delay': timedelta(minutes=5),  
    'depends_on_past': False,
    'catchup':False
}

with DAG(
        'project_6_load_to_Vertica_STG',                     
        default_args=default_args,         
        schedule_interval='0/15 * * * *',  
        start_date=datetime(2021, 1, 1),   
        catchup=False,                     
        tags=['sprint6', 'example'],
) as dag:

    # create DAG logic (sequence/order)
    t1 = DummyOperator(task_id="start")
    t2 = PythonOperator(task_id="create_tables_and_load_data", python_callable=execute_vertica, dag=dag)
    t3 = DummyOperator(task_id="end")
    
    t1 >> t2 >> t3
