from airflow import DAG
from airflow.macros import *

import os
from glob import glob
import logging
import subprocess

from plugins.redshift_summary import RedshiftSummaryOperator


def load_all_table_conf_into_list(path_to_json):

    configs = []
    for f_name in glob(path_to_json+ '/*.py'):
        # logging.info(f_name)
        with open(f_name) as f:
            dict_text = f.read()
            try:
                dict = eval(dict_text)
            except Exception as e:
                logging.info(str(e))
                raise
            else:
                configs.append(dict)

    return configs


def find_table(table_name, table_confs):
    """
    scan through table_confs and see if there is a table matching table_name
    """
    for table in table_confs:
        if table.get("table") == table_name:
            return table

    return None


DAG_ID = "Build_Summary_Tables"
dag = DAG(
    DAG_ID,
    schedule_interval="25 13 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2020, 8, 14)
)

# this should be listed in dependency order (all in analytics)
tables_load = [
    'user_summary'
]

dag_root_path = os.path.dirname(os.path.abspath(__file__))
logging.info(dag_root_path)
table_confs = load_all_table_conf_into_list(dag_root_path + "/config/")

prev_task = None
for table_name in tables_load:

    table = find_table(table_name, table_confs)

    summarizer = RedshiftSummaryOperator(
            table=table["table"],
            schema=table["schema"],
            redshift_conn_id="redshift_dev_db",
            input_check=table["input_check"],
            main_sql=table["main_sql"],
            output_check=table["output_check"],
            overwrite=True,
            after_sql=table.get("after_sql"),
            pre_sql=table.get("pre_sql"),
            attributes=table.get("attributes", ""),
            dag=dag,
            task_id="anayltics"+"__"+table["table"]
    )
    if prev_task is not None:
        prev_task >> summarizer
    prev_task = summarizer
