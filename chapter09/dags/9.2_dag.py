
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator


dag = DAG(
    dag_id="9.2_dag",
    start_date=datetime(2023, 10, 1),
    schedule = '@daily',
)

t1 = DummyOperator(task_id='t1', dag=dag)
t2 = DummyOperator(task_id='t2', dag=dag)
t3 = DummyOperator(task_id='t3', dag=dag)

t1 >> t2 >> t3 >> t1