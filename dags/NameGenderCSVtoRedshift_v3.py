from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = ""  # 본인 ID 사용
    redshift_pass = ""  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    return (f.text)


def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")
    return lines


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    lines = iter(lines)
    next(lines)
    sql = "BEGIN; DELETE FROM TABLE {schema}.{table};"
    for line in lines:
        if line != "":
            (name, gender) = line.split(",")
            print(name, "-", gender)
            sql += """INSERT INTO raw_data.name_gender VALUES ('{name}', '{gender}');""".format(name=name, gender=gender)
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


dag_second_assignment = DAG(
    dag_id = 'second_assignment_v3',
    start_date = datetime(2020,8,10), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url':  Variable.get("csv_url")
    },
    provide_context=True,
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'raw_data',
        'table': 'name_gender'
    },
    provide_context=True,
    dag = dag_second_assignment)

extract >> transform >> load
