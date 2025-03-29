import os
import json
import pandas as pd
import logging
import requests
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

MOVIELENS_HOST = os.environ.get("MOVIELENS_HOST", "192.168.0.231")
MOVIELENS_SCHEMA = os.environ.get("MOVIELENS_SCHEMA", "http")
MOVIELENS_PORT = os.environ.get("MOVIELENS_PORT", "5000")

MOVIELENS_USER = os.environ.get("MOVIELENS_USER", "airflow")
MOVIELENS_PASSWORD = os.environ.get("MOVIELENS_PASSWORD", "airflow")


dag = DAG(
    dag_id="8.1_dag_practice",
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 1, 3),
    schedule_interval="@daily",
)


# =========================
# Task 1: Fetch ratings
# =========================

def _get_session():
    session = requests.Session()
    session.auth = (MOVIELENS_USER, MOVIELENS_PASSWORD)  # Replace with your credentials

    base_url = f"{MOVIELENS_SCHEMA}://{MOVIELENS_HOST}:{MOVIELENS_PORT}"

    return session, base_url



def _get_with_pagination(session, url, params, batch_size=100):
    
    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(url,
                               params={
                                   **params,
                                   **{"offset": offset, "limit": batch_size}
                               })
        response.raise_for_status()
        response_json = response.json()

        yield from response_json["result"] # list

        offset += batch_size
        total = response_json["total"]




def _get_ratings(start_date, end_date, batch_size=100):
    session, base_url = _get_session()

    yield from _get_with_pagination(  # generator
        session=session,
        url=f"{base_url}/ratings",
        params={
            "start_date": start_date,
            "end_date": end_date,
        },
        batch_size=batch_size,
    )



def _fetch_ratings(templates_dict, batch_size=1000, **_):
    logger = logging.getLogger(__name__)
    
    start_date = templates_dict["start_date"]
    end_date = templates_dict["end_date"]
    output_path = templates_dict["output_path"]
    logger.info(f"Fetching ratings from {start_date} to {end_date} into {output_path}")

    ratings = list(
        _get_ratings(start_date, end_date, batch_size=batch_size)
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