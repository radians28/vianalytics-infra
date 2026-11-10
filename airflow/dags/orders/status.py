"""Final upload_progress status once an upload's orders are loaded.

Completed: no listing of the upload waits in royal_canin.log_sku_match (PENDING)
Paused   : listings wait for the SKU review. After the review, handle_continue_order_data_{channel}
           loads their order lines and calls finalize again (meta_key='orders_continue'), which moves
           the upload to Completed
Paused + resume_needed: nothing is pending, but a listing of the upload was resolved while this
           load ran (in the review of another upload, or by another upload's SKU step), after its
           lines were skipped; a Resume loads them
Failed is set by staging.common.fail_upload (on_failure_callback, after the last retry).

A review row is shared by every upload holding the listing (log_sku_match_upload), so the counts
go through that link, not log_sku_match.upload_id.
"""
import json
from contextlib import closing

from airflow.providers.postgres.hooks.postgres import PostgresHook

from staging.common import SCHEMA

# resolutions whose order lines are loaded by handle_continue_order_data (auto_created ones are
# loaded by the first order load; rematch_created ones were PENDING then, so their lines wait)
CONTINUE_RESOLUTIONS = ('manual', 'sibling', 'auto_mapped', 'rematch_created')


def finalize(result: dict, conn_id: str = 'vianalytics_db', meta_key: str = 'orders') -> dict:
    """meta_key: where this load's counts go in meta_data (the continue DAG keeps the first load's)."""
    upload_id = result['upload_id']
    first_load = meta_key == 'orders'
    hook = PostgresHook(postgres_conn_id=conn_id)
    with closing(hook.get_conn()) as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT count(*) FILTER (WHERE m.status = 'PENDING'),
                   count(*) FILTER (WHERE m.status <> 'PENDING' AND m.resolution = ANY(%s))
            FROM {SCHEMA}.log_sku_match_upload mu JOIN {SCHEMA}.log_sku_match m USING (match_id)
            WHERE mu.upload_id = %s
        """, (list(CONTINUE_RESOLUTIONS), upload_id))
        pending, resolved = cur.fetchone()
        # first load only: a listing linked to this upload got resolved after its lines were skipped
        skipped = sum((c or {}).get('skipped_pending_lines', 0) for c in (result.get('orders') or {}).values())
        resume_needed = first_load and not pending and resolved > 0 and skipped > 0
        status = 'Paused' if pending or resume_needed else 'Completed'
        cur.execute("""
            UPDATE upload_progress
            SET status = %(status)s, error_message = NULL,
                meta_data = ((coalesce(meta_data::jsonb, '{}'::jsonb) - 'resume_needed')
                             || jsonb_build_object('sku_review_pending', %(pending)s,
                                                   %(meta_key)s::text, %(orders)s::jsonb)
                             || CASE WHEN %(resume_needed)s THEN jsonb_build_object('resume_needed', true)
                                     ELSE '{}'::jsonb END)::json,
                finished_at = now(), last_heartbeat = now(), updated_at = now()
            WHERE upload_id = %(upload_id)s
        """, {'status': status, 'pending': pending, 'upload_id': upload_id, 'meta_key': meta_key,
              'orders': json.dumps(result.get('orders') or {}), 'resume_needed': resume_needed})
        conn.commit()
    return {'upload_id': upload_id, 'status': status, 'sku_review_pending': pending,
            'resume_needed': resume_needed}
