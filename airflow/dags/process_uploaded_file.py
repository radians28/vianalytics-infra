from contextlib import closing
from datetime import datetime
from typing import Any


from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, get_current_context

@dag(
    dag_id="api_triggered_dag",
    schedule=None,              # on-demand only, triggered by the API
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=10,         # limit concurrent requests
    tags=["api", "on-demand"],
    default_args={"retries": 2},
)
def trigger_file_to_staging():
    @task
    def get_run_data() -> dict:
        ctx = get_current_context()
        conf = ctx["dag_run"].conf or {}
        if 'upload_id' not in conf:
            raise ValueError('Missing upload_id to be run')

        import uuid
        try:
            upload_id = str(uuid.UUID(str(conf['upload_id'])))
        except ValueError:
            raise ValueError('Invalid upload_id format')

        hook = PostgresHook(postgres_conn_id="vianalytics_db")
        # The postgres provider may hand back a psycopg2 or a psycopg 3
        # connection, so rows are mapped to dicts by hand instead of using a
        # driver-specific cursor/row factory
        with closing(hook.get_conn()) as conn, conn.cursor() as cur:
            try:
                cur.execute("""
                    UPDATE upload_progress
                    SET status = 'Processing',
                        processed_at = NOW()
                    WHERE upload_id = %(upload_id)s
                    RETURNING
                        upload_id,
                        file_name,
                        file_path,
                        channel,
                        status
                """, { 'upload_id': upload_id })
                row = cur.fetchone()
                columns = [col[0] for col in cur.description]
                conn.commit()
            except Exception:
                conn.rollback() # pyright: ignore[reportAttributeAccessIssue]
                raise Exception(f'Failed to process the query record for {upload_id}')
        if row is None:
            raise Exception(f'Queue {upload_id} does not exist')
        return dict(zip(columns, row))

    @task.branch
    def choose_channel_processor(channel: str) -> str:
        routes = {
            'tiktok_tokped': 'process_stg_tiktok_tokped',
            'shopee': 'process_stg_shopee',
            'lazada': 'process_stg_lazada',
            'blibli': 'process_stg_blibli',
        }
        try:
            return routes[channel]
        except KeyError:
            raise AirflowFailException(f"Unsupported channel: {channel!r}")

    """
        TODO
        process_stg_{channel} step by step
        1. Create connection for both postgreSQL and excel/csv file
        2. Update upload progress state to processing
        3. Stream read excel/csv file, bulk insert every 1000 records
    """
    @task
    def process_stg_tiktok_tokped(upload: dict) -> dict:
        from staging import tiktok_tokped
        return tiktok_tokped.load(upload)

    @task
    def process_stg_shopee(upload: dict) -> dict:
        return upload

    @task
    def process_stg_lazada(upload: dict) -> dict:
        from staging import lazada
        return lazada.load(upload)

    @task
    def process_stg_blibli(upload: dict) -> dict:
        from staging import blibli
        return blibli.load(upload)

    """
        TODO
        process_sku_{channel} step by step
        1. Create postgreSQL connection and stream read from stg channel in chunk/paginated, 
           only select record related to master data SKU -> reduce memory bottleneck
        2. Check if the SKU keyset has been seen in this process, skip it (SKU consolidation only 
           process master data not transactional, therefore there is no reqired check for same SKU 
           as previous one)
        3. For unseen SKU, validate record with existing one
           a. SKU history for seller SKU
           b. SKU platform for platform SKU + channel
        4. If one or all of them exist (point 3a and 3b), then record already exist
           a. 3a exists but 3b doesn't exist, SKU was registered from other platform, insert to SKU platform for this one
           b. 3a doesn't exist but 3b exists, SKU has been updated to new one/this record is the old one
              insert to SKU history and update SKU master metadata based on timeline (compare the order date and SKU seller effective date)
        5. If both of them doesn't exist, process either as new SKU record or map to existing one (based on proposed process)
    """
    @task
    def process_sku_tiktok_tokped(upload: dict) -> dict:
        return upload

    @task
    def process_sku_shopee(upload: dict) -> dict:
        return upload
    
    @task
    def process_sku_lazada(upload: dict) -> dict:
        return upload
    
    @task
    def process_sku_blibli(upload: dict) -> dict:
        return upload

    upload_file: dict[str, Any] = get_run_data() # pyright: ignore[reportAssignmentType]
    channel = upload_file['channel']
    branching = choose_channel_processor(channel)

    stg_tiktok_tokped: dict[str, Any] = process_stg_tiktok_tokped(upload_file) # pyright: ignore[reportAssignmentType]
    stg_shopee: dict[str, Any] = process_stg_shopee(upload_file) # pyright: ignore[reportAssignmentType]
    stg_lazada: dict[str, Any] = process_stg_lazada(upload_file) # pyright: ignore[reportAssignmentType]
    stg_blibli: dict[str, Any] = process_stg_blibli(upload_file) # pyright: ignore[reportAssignmentType]

    branching >> [ # type: ignore[operator]
            stg_tiktok_tokped,
            stg_shopee,
            stg_lazada,
            stg_blibli,
    ]

    sku_tiktok_tokped: dict[str, Any] = process_sku_tiktok_tokped(stg_tiktok_tokped) # pyright: ignore[reportAssignmentType]
    sku_shopee: dict[str, Any] = process_sku_shopee(stg_shopee) # pyright: ignore[reportAssignmentType]
    sku_lazada: dict[str, Any] = process_sku_lazada(stg_lazada) # pyright: ignore[reportAssignmentType]
    sku_blibli: dict[str, Any] = process_sku_blibli(stg_blibli) # pyright: ignore[reportAssignmentType]

    done = TriggerDagRunOperator(
        task_id="done",
        trigger_dag_id=f'handle_order_dag_{channel}',
        trigger_rule="none_failed_min_one_success",
        wait_for_completion=False,
        conf=upload_file
    )

    [
        sku_tiktok_tokped,
        sku_blibli,
        sku_lazada,
        sku_shopee
    ] >> done

    

trigger_file_to_staging()