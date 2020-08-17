from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,14), # 적당히 조절
	schedule_interval = '0 2 * * *')  # 적당히 조절


def etl():
	# 여기에 코드를 기록
	pass
   


task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
  
# task가 하나 밖에 없는 경우 아무 것도 하지 않아도 그냥 실행됨
