from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = ""  # 하드코딩 노노 Airflow UI -> Admin -> Connections -> Create 
    redshift_pass = ""  
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


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return (f.text)


def transform(text):
    logging.info("transform started")
    # ignore the first line - header
    lines = text.split("\n")[1:]
    logging.info("transform done")
    return lines


def load(lines):
    logging.info("load started")
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM TABLE raw_data.name_gender;" # 트랜잭션 - sql구문 시작과 끝에 BEGIN, END # DELETE FROM -> TRUNCATE과 같지만 TRUNCATE은 롤백이 안 된다.
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += "INSERT INTO raw_data.name_gender VALUES ('{name}', '{gender}');"
    sql += "END;" # END -> COMMIT도 가능
    cur.execute(sql) # try, except로 감싸지 않는 이유는 Airflow에서는 Fail로
    logging.info(sql)
    logging.info("load done")


def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)


dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,10), # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 2 * * *')  # 적당히 조절

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
