"""Final upload_progress status once an upload's orders are loaded.

Completed: no listing of the upload waits in royal_canin.log_sku_match (PENDING)
Paused   : listings wait for the SKU review; the pending-SKU pipeline loads their order lines and
           moves the upload to Completed when the last one is resolved
Failed is set by staging.common.fail_upload (on_failure_callback, after the last retry).
"""
import json
from contextlib import closing

from airflow.providers.postgres.hooks.postgres import PostgresHook

from staging.common import SCHEMA


def finalize(result: dict, conn_id: str = 'vianalytics_db') -> dict:
    upload_id = result['upload_id']
    hook = PostgresHook(postgres_conn_id=conn_id)
    with closing(hook.get_conn()) as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT count(*) FROM {SCHEMA}.log_sku_match
            WHERE upload_id = %s AND status = 'PENDING'
        """, (upload_id,))
        pending = cur.fetchone()[0]
        status = 'Paused' if pending else 'Completed'
        cur.execute("""
            UPDATE upload_progress
            SET status = %(status)s, error_message = NULL,
                meta_data = (coalesce(meta_data::jsonb, '{}'::jsonb)
                             || jsonb_build_object('sku_review_pending', %(pending)s,
                                                   'orders', %(orders)s::jsonb))::json,
                finished_at = now(), last_heartbeat = now(), updated_at = now()
            WHERE upload_id = %(upload_id)s
        """, {'status': status, 'pending': pending, 'upload_id': upload_id,
              'orders': json.dumps(result.get('orders') or {})})
        conn.commit()
    return {'upload_id': upload_id, 'status': status, 'sku_review_pending': pending}
