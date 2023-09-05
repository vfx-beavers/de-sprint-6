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
    logging.info("Start connection")
    cur.execute("""
DELETE from STV2023081241__DWH.h_users;
DELETE from STV2023081241__DWH.h_groups;
DELETE from STV2023081241__DWH.l_user_group_activity;
DELETE from STV2023081241__DWH.s_auth_history;

----- пользователи

create table IF NOT EXISTS STV2023081241__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2023081241__DWH.h_users(hk_user_id, user_id, load_dt, load_src)
select distinct 
       hash(user_id) as  hk_user_id,
       user_id,
       now() as load_dt,
       's3' as load_src
       from STV2023081241__STAGING.group_log
where hash(user_id) not in (select hk_user_id from STV2023081241__DWH.h_users);

------- группы

create table IF NOT EXISTS STV2023081241__DWH.h_groups
(
    hk_group_id       bigint primary key,
    group_id          int,
    registration_dt datetime,
    load_dt           datetime,
    load_src          varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2023081241__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select distinct 
       hash(group_id) as hk_group_id,
       group_id,
       "datetime", 
       now() as load_dt,
       's3' as load_src
       from STV2023081241__STAGING.group_log
where hash(group_id) not in (select hk_group_id from STV2023081241__DWH.h_groups)
and event = 'create';

----- пользовательская активность линк

create table IF NOT EXISTS STV2023081241__DWH.l_user_group_activity
(
	hk_l_user_group_activity bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_h_users REFERENCES STV2023081241__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_h_groups REFERENCES STV2023081241__DWH.h_groups (hk_group_id),
	load_dt           datetime,
	load_src          varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2023081241__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct 
    hash(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from STV2023081241__STAGING.group_log as gl
    left join STV2023081241__DWH.h_users as hu on gl.user_id = hu.user_id
    left join STV2023081241__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV2023081241__DWH.l_user_group_activity);

----- история авторизаций сат

create table IF NOT EXISTS STV2023081241__DWH.s_auth_history
(hk_l_user_group_activity bigint not null CONSTRAINT fk_l_user_group_activity REFERENCES STV2023081241__DWH.l_user_group_activity (hk_l_user_group_activity)
, user_id_from integer, event varchar(100), event_dt datetime, load_dt datetime, load_scr varchar(20))
order by event_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);

INSERT INTO STV2023081241__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_scr)
select distinct   
	luga.hk_l_user_group_activity
	, gl.user_id_from
	, gl.event
	, gl."datetime" 
	, now() as load_dt
	, 's3' as load_src
from STV2023081241__STAGING.group_log as gl
left join STV2023081241__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023081241__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023081241__DWH.l_user_group_activity as luga on (hg.hk_group_id = luga.hk_group_id) and (hu.hk_user_id = luga.hk_user_id);
        """)
    for i in ['h_users', 'h_groups', 'l_user_group_activity', 's_auth_history']:    
        sql = f"select count(*) from STV2023081241__DWH.{i}"
        cur.execute(sql)
        result = cur.fetchall()[0][0]
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
        'Project_6_load_to_Vertica_DDS',             
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
