from datetime import datetime
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, get_current_context

@dag(
    dag_id="handle_bundle_record",
    schedule=None,              # on-demand only, triggered by the API
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=10,         # limit concurrent requests
    tags=["api", "on-demand"],
    default_args={"retries": 2},
)
def handle_bundle_record():
    @task
    def handle_record_params() -> dict:
        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}

        if 'file_path' not in conf:
            raise ValueError('Uploaded file is missing')

        return { 'file_path': str(conf['file_path']) }

    @task
    def parse_stream_and_insert(param: dict):
        # TODO read stream uploaded bundle record, then bulk insert data, 
        # detail SKU should be validated to master SKU data
        pass

    done = EmptyOperator(
        task_id='done'
    )

    params: dict[str, Any] = handle_record_params() # pyright: ignore[reportAssignmentType]

    parse_stream_and_insert(params) >> done

handle_bundle_record()
