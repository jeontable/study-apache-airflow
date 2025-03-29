import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="06_templated_query", 
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 1, 3),
    schedule_interval=datetime.timedelta(days=1),
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="curl -o /tmp/events.json -L http://192.168.0.231:5000/events?start_date={{ execution_date.strftime('%Y-%m-%d') }}&end_date={{ next_execution_date.strftime('%Y-%m-%d') }}",
    dag=dag, 
)


def _calculate_events(input_path, output_path):
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="total")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_events = PythonOperator( 
    task_id="calculate_events",
    python_callable=_calculate_events,
    op_kwargs={
        "input_path": "/tmp/events.json",
        "output_path": "/tmp/stats.csv",
    },
    dag=dag,
)

fetch_events >> calculate_events