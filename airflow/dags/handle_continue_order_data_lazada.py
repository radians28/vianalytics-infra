"""Order lines of the lazada SKUs an admin resolved in the SKU review -> royal_canin.t_order / t_order_detail.

Triggered by vianalytics-svc (POST /api/upload/<id>/sku-review/resolve or /resume) with conf
{upload_id, channel, match_ids}. Loads ONLY the lines of those manually resolved listings
(orders/load.py run_continue): lines the first load wrote, auto-created SKUs included, and existing order
headers are left as they are. Runs one at a time; it also shares the per-channel order lock with
handle_order_dag_lazada.
"""
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, get_current_context

from staging.common import fail_upload


@dag(
    dag_id='handle_continue_order_data_lazada',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['orders', 'continue', 'lazada'],
    # upload_progress -> 'Failed' only once a task has used up its retries
    default_args={'retries': 2, 'on_failure_callback': fail_upload},
)
def handle_continue_order_data_lazada():
    @task
    def load_resolved_orders() -> dict:
        from orders import load
        return load.run_continue(get_current_context()['dag_run'].conf or {})

    @task
    def finalize_upload(result: dict) -> dict:
        from orders import status
        return status.finalize(result, meta_key='orders_continue')

    done = EmptyOperator(task_id='done')

    finalize_upload(load_resolved_orders()) >> done  # pyright: ignore[reportArgumentType]


handle_continue_order_data_lazada()
