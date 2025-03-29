import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="08_templated_path", 
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 1, 3),
    schedule_interval=datetime.timedelta(days=1),
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="curl -o /tmp/events{{ds}}.json -L http://192.168.0.231:5000/events?start_date={{ ds }}&end_date={{ next_ds }}",
    dag=dag, 
)


def _calculate_events(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="total")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_events = PythonOperator( 
    task_id="calculate_events",
    python_callable=_calculate_events,
    templates_dict={
        "input_path": "/tmp/events{{ds}}.json",
        "output_path": "/tmp/stats{{ds}}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_events