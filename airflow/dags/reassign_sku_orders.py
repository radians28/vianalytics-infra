"""Move the stored order lines after a SKU merge or split (orders/reassign.py).

Triggered by vianalytics-svc (SKU Master page: Merge / Split, or Retry in the changes drawer) with conf
{change_id}. svc has already changed the SKU master and logged the change in royal_canin.log_sku_change;
this DAG re-points the order lines, re-splits bundle lines where needed and, for a merge, deletes the
merged-away SKU. One run at a time; each write takes the channel's order lock, so it never overlaps an
order load. Afterwards the listings waiting for SKU review are re-matched (rematch_pending_skus).
"""
from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id='reassign_sku_orders',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['orders', 'sku', 'api'],
    default_args={'retries': 1},
)
def reassign_sku_orders():
    @task
    def reassign_orders() -> dict:
        from orders import reassign
        return reassign.run(get_current_context()['dag_run'].conf or {})

    rematch = TriggerDagRunOperator(
        task_id='rematch',
        trigger_dag_id='rematch_pending_skus',
        conf={'source': 'sku_change', 'by': "{{ dag_run.conf.get('by', 'auto') }}"},
        wait_for_completion=False,
    )

    reassign_orders() >> rematch  # pyright: ignore[reportOperatorIssue]


reassign_sku_orders()
