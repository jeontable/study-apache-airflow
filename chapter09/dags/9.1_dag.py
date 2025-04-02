
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


dag = DAG(
    dag_id="9.1_dag",
    start_date=datetime(2023, 10, 1),
    schedule_interval='@daily',
)

t1 = EmptyOperator(task_id='t1', dag=dag)
t2 = EmptyOperator(task_id='t2', dag=dag)
t3 = EmptyOperator(task_id='t3', dag=dag)

t1 >> t2 >> t3 >> t1