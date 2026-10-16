from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

@dag(
    dag_id='handle_order_dag_blibli',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def handle_order_dag_blibli():
    @task
    def do_nothing():
        pass

    done = EmptyOperator(
        task_id='done'
    )

    do_nothing() >> done

handle_order_dag_blibli()