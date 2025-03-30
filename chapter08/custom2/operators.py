
import os
import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom2.hooks import MovielensHook



class MovielensFetchRatingOperator(BaseOperator):
    """
    Custom operator to fetch movie ratings from the Movielens dataset.
    """

    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(self, conn_id, output_path, start_date="{{ds}}", end_date="{{next_ds}}", **kwargs):
        super(MovielensFetchRatingOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._output_path = output_path


    def execute(self, context):
        # Logic to fetch movie ratings
        hook = MovielensHook(conn_id=self._conn_id)

        try:
            self.log.info(f"Fetching ratings from {self._start_date} to {self._end_date} into {self._output_path}")
            ratings = list(
                hook.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                )
            )
            self.log.info(f"Fetched {len(ratings)} ratings")
        finally:
            hook.close()

        self.log.info(f"Writing ratings to {self._output_path}")

        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)
        with open(self._output_path, "w") as f:
            json.dump(ratings, f)
        self.log.info(f"Ratings written to {self._output_path}")