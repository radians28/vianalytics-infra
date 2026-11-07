"""Re-split order lines already loaded after a SKU bundling upload changed bundle components.

Triggered by vianalytics-svc (bundle upload with "re-split stored orders", or the SKU Bundling page)
with conf {change_ids}: royal_canin.log_bundle_change rows holding the old and new component lists
(orders/rebundle.py). One run at a time; each write batch also takes the channel's order lock, so it
never overlaps handle_order_dag_* / handle_continue_order_data_* writing the same orders. A bundle
that fails is marked FAILED on its change row and can be retried; the others still run.
"""
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id='reconsolidate_bundle_orders',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['orders', 'bundle', 'api'],
    default_args={'retries': 1},
)
def reconsolidate_bundle_orders():
    @task
    def rebundle_orders() -> dict:
        from orders import rebundle
        return rebundle.run(get_current_context()['dag_run'].conf or {})

    done = EmptyOperator(task_id='done')

    rebundle_orders() >> done  # pyright: ignore[reportOperatorIssue]


reconsolidate_bundle_orders()
