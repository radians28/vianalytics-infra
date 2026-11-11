"""Re-match the listings waiting for SKU review against the current master (sku/rematch.py).

Triggered with conf {source, upload_id, by}:
- by vianalytics-svc after a SKU review is resolved (source 'review')
- by handle_order_dag_{channel} once an upload's order load has finished (source 'upload')

1. rematch:        every PENDING listing is mapped (master knows it now), given a new SKU (no close
                   name any more) or a refreshed candidate list (still close: a person decides)
2. mark_resuming:  Paused uploads left with nothing pending are claimed (-> Processing)
3. resume_uploads: handle_continue_order_data_{channel} per claimed upload loads the skipped lines;
                   if a trigger fails the upload goes back to Paused with resume_error (svc Resume)

One run at a time (runs queue); the continue / order DAGs run alongside (see sku/rematch.py).
"""
import json
from contextlib import closing
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, get_current_context

CONTINUE_DAG_ID = 'handle_continue_order_data_{channel}'


def _back_to_paused(context):
    """A continue DAG could not be triggered: the upload waits for a manual Resume again."""
    ti = context['ti']
    targets = ti.xcom_pull(task_ids='mark_resuming') or []
    if ti.map_index is None or ti.map_index < 0 or ti.map_index >= len(targets):
        return
    upload_id = targets[ti.map_index]['conf']['upload_id']
    error = str(context.get('exception') or 'trigger failed')[:500]
    hook = PostgresHook(postgres_conn_id='vianalytics_db')
    with closing(hook.get_conn()) as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE upload_progress
            SET status = 'Paused', updated_at = now(),
                meta_data = (coalesce(meta_data::jsonb, '{}'::jsonb)
                             || jsonb_build_object('resume_error', %s::text))::json
            WHERE upload_id = %s AND status = 'Processing'
        """, (error, upload_id))
        conn.commit()


@dag(
    dag_id='rematch_pending_skus',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sku', 'review', 'rematch'],
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=1)},
)
def rematch_pending_skus():
    @task
    def rematch() -> dict:
        from sku import rematch as rm
        return rm.run(get_current_context()['dag_run'].conf or {})

    @task
    def mark_resuming(result: dict) -> list[dict]:
        """Claim each Paused upload (only if still Paused) and build its continue-DAG trigger."""
        stamp = datetime.now().strftime('%Y%m%d%H%M%S')
        targets = []
        hook = PostgresHook(postgres_conn_id='vianalytics_db')
        with closing(hook.get_conn()) as conn, conn.cursor() as cur:
            for r in result.get('resume') or []:
                dag_id = CONTINUE_DAG_ID.format(channel=r['channel'])
                run_id = f"continue__{r['upload_id']}__{stamp}__rematch"
                resume_meta = {'dag_id': dag_id, 'dag_run_id': run_id, 'by': 'auto (rematch)',
                               'triggered_at': datetime.now().astimezone().isoformat()}
                cur.execute("""
                    UPDATE upload_progress
                    SET status = 'Processing', error_message = NULL, processed_at = now(),
                        last_heartbeat = now(), updated_at = now(),
                        meta_data = ((coalesce(meta_data::jsonb, '{}'::jsonb) - 'resume_error' - 'resume_needed')
                                     || jsonb_build_object('resume', %s::jsonb))::json
                    WHERE upload_id = %s AND status = 'Paused'
                    RETURNING upload_id
                """, (json.dumps(resume_meta), r['upload_id']))
                if cur.fetchone():
                    targets.append({
                        'trigger_dag_id': dag_id,
                        'trigger_run_id': run_id,
                        'conf': {'upload_id': r['upload_id'], 'channel': r['channel'], 'match_ids': r['match_ids']},
                    })
            conn.commit()
        return targets

    targets = mark_resuming(rematch())

    TriggerDagRunOperator.partial(
        task_id='resume_uploads',
        wait_for_completion=False,
        retries=1,
        on_failure_callback=_back_to_paused,
    ).expand_kwargs(targets)


rematch_pending_skus()
