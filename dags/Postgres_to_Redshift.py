from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from plugins.postgres_to_s3_operator import PostgresToS3Operator
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator


DAG_ID = "Postgres_to_Redshift"
dag = DAG(
    DAG_ID,
    schedule_interval="@once", # 웹UI나 터미널에서 Trigger 해줘야 실행되는 상태, 개발 시 자주 쓰는 모드
    max_active_runs=1,
    concurrency=2,
    start_date=datetime(2020, 8, 10),
    catchup=False
)

tables = [
    "customer_features",
    "customer_variants"
    # 테이블 추가해야
]


# s3_bucket, local_dir
s3_bucket = 'grepp-data-engineering'
local_dir = './'           # 실제 프로덕션에서는 공간이 충분한 폴더 (volume)로 맞춰준다
s3_key_prefix = 'yongjunlee'  # 본인의 ID에 맞게 수정
schema = 'raw_data'        # 본인이 사용하는 스키마에 맞게 수정
prev_task = None

for table in tables:
    s3_key=s3_key_prefix+'/'+table+'.tsv'

    postgrestos3 = PostgresToS3Operator(
        table="public."+table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        data_dir=local_dir,
        dag=dag,
        task_id="Postgres_to_S3"+"_"+table
    )

    s3toredshift = S3ToRedshiftOperator(
        schema=schema,
        table=table,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        copy_options="delimiter '\\t' COMPUPDATE ON", # permission 설정
        aws_conn_id='aws_s3_default',
        task_id='S3_to_Redshift'+"_"+table,
        dag=dag
    )
    if prev_task is not None:
        prev_task >> postgrestos3 >> s3toredshift
    else:
        postgrestos3 >> s3toredshift
    prev_task = s3toredshift
