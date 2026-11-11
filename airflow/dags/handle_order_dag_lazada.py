"""Orders of an uploaded lazada export -> royal_canin.t_order / t_order_detail (orders/load.py).

Triggered by api_triggered_dag's `done` task with the upload as conf. Ends by triggering rematch_pending_skus. Runs one at a time
(max_active_runs=1): a second lazada upload waits queued, other marketplaces run alongside.
"""
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, get_current_context

from staging.common import fail_upload


@dag(
    dag_id='handle_order_dag_lazada',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['orders', 'lazada'],
    # upload_progress -> 'Failed' only once a task has used up its retries
    default_args={'retries': 2, 'on_failure_callback': fail_upload},
)
def handle_order_dag_lazada():
    @task
    def load_orders() -> dict:
        from orders import load
        return load.run(get_current_context()['dag_run'].conf or {})

    @task
    def finalize_upload(result: dict) -> dict:
        from orders import status
        return status.finalize(result)

    done = EmptyOperator(task_id='done')

    # the listings still waiting for review are re-matched against the master this upload changed;
    # a failed trigger never fails the upload (its orders are loaded)
    rematch = TriggerDagRunOperator(
        task_id='rematch',
        trigger_dag_id='rematch_pending_skus',
        conf={'source': 'upload', 'upload_id': "{{ dag_run.conf['upload_id'] }}"},
        wait_for_completion=False,
        on_failure_callback=None,
    )

    finalize_upload(load_orders()) >> rematch >> done  # pyright: ignore[reportArgumentType]


handle_order_dag_lazada()
