import os
import json
import pandas as pd
import logging
import requests
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from custom.hooks import MovielensHook


dag = DAG(
    dag_id="8.2.dag_using_hook",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 1, 3),
    schedule_interval="@daily",
)


def _fetch_ratings(templates_dict, batch_size=1000, **_):
    logger = logging.getLogger(__name__)
    
    start_date = templates_dict["start_date"]
    end_date = templates_dict["end_date"]
    output_path = templates_dict["output_path"]
    conn_id = templates_dict["conn_id"]
    logger.info(f"Fetching ratings from {start_date} to {end_date} into {output_path}")

    hook = MovielensHook(conn_id=conn_id)

    ratings = list(
        hook.get_ratings(
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size
        )
    )

    logger.info(f"Fetched {len(ratings)} ratings")

    logger.info(f"Writing ratings to {output_path}")

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(ratings, f)



fetch_ratings = PythonOperator(
    task_id="fetch_ratings",
    python_callable=_fetch_ratings,
    templates_dict={
        "start_date": "{{ ds }}",
        "end_date": "{{ next_ds }}",
        "output_path": "/tmp/practice/ratings{{ ds }}.json",
        "conn_id": "movielens",
    },
    dag=dag,
)





# =========================
# Task 2: Rank movies by rating
# =========================


def rank_movies_by_rating_df(ratings, min_ratings=2):
    ranking = (
        ratings.groupby("movieId").agg(
            avg_rating=pd.NamedAgg(column="rating", aggfunc="mean"),
            num_ratings=pd.NamedAgg(column="userId", aggfunc="count")
        ).loc[lambda df: df["num_ratings"] > 2].sort_values(
            ["avg_rating", "num_ratings"],
            ascending=True
        )
    )
    return ranking


def _rank_movies(templates_dict, min_ratings=2, **_):
    input_path = templates_dict["input_path"]
    output_path = templates_dict["output_path"]

    rankings = pd.read_json(input_path)
    ranking = rank_movies_by_rating_df(rankings, min_ratings=min_ratings)

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    ranking.to_json(output_path, index=True)


rank_movies_by_rating = PythonOperator(
    task_id="rank_movies_by_rating",
    python_callable=_rank_movies,
    templates_dict={
        "input_path": "/tmp/practice/ratings{{ ds }}.json",
        "output_path": "/tmp/practice/rankings{{ ds }}.json",
    }
)



fetch_ratings >> rank_movies_by_rating