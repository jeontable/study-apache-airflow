from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from custom2.hooks import MovielensHook



class MovielensRatingsSensor(BaseSensorOperator):

    template_fields = ("_start_date", "_end_date")

    @apply_defaults
    def __init__(self, conn_id, start_date="{{ds}}", end_date="{{next_ds}}", **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date


    def poke(self, context):
        hooks = MovielensHook(conn_id=self._conn_id)

        try:
            next(
                hooks.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    batch_size=1
                )
            )
            self.log.info(f"Found ratings for {self._start_date} to {self._end_date}")
            return True
        except StopIteration:
            self.log.info(f"No ratings found for {self._start_date} to {self._end_date}")
            self.log.info("Sleeping ...")
            return False
        except Exception as e:
            self.log.error(f"Error while checking for ratings: {e}")
            return False
        finally:
            hooks.close()