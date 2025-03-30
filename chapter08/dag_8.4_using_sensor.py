import os
import json
import pandas as pd
import logging
import requests
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from custom2.operators import MovielensFetchRatingOperator
from custom2.sensors import MovielensRatingsSensor



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


with DAG( 
    dag_id="8.4_dag_using_sensor", 
    start_date=datetime.datetime(2019, 1, 1),
    end_date=datetime.datetime(2019, 1, 3),
    schedule_interval="@daily"
) as dag:

    wait_for_ratings = MovielensRatingsSensor( 
        task_id="wait_for_ratings",
        conn_id="movielens",
        start_date="{{ ds }}",
        end_date="{{ next_ds }}"
    )

    fetch_ratings = MovielensFetchRatingOperator(
        task_id="fetch_ratings",
        conn_id="movielens",
        start_date="{{ ds }}",
        end_date="{{ next_ds }}",
        output_path="/tmp/practice/ratings{{ ds }}.json"
    )

    rank_movies_by_rating = PythonOperator(
        task_id="rank_movies_by_rating",
        python_callable=_rank_movies,
        templates_dict={
            "input_path": "/tmp/practice/ratings{{ ds }}.json",
            "output_path": "/tmp/practice/rankings{{ ds }}.json"
        }
    )

    wait_for_ratings >> fetch_ratings >> rank_movies_by_rating 