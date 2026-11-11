"""reassign_sku_orders: move the stored order lines after a SKU merge or split (svc sku_change.py).

svc has already changed the master (seller SKUs, listings, history, bundle components) and logged the
change in royal_canin.log_sku_change. For each PENDING / FAILED change, oldest first:

1. plain lines
   MERGE B -> A: every row with sku_id B (plain lines, and component rows of bundles that list B) -> A
   SPLIT A -> T: every plain row of A is resolved again with the order load's rule (cleaned seller SKU
   via m_sku_alias / m_sku_history, else the listing, orders.rebundle.Lookups.line_sku); rows that now
   resolve to T move. Rows are updated per channel under that channel's order lock, so an order load
   never writes the same orders at the same time.
2. bundle lines (log_sku_change.bundle)
   rebuild: {sku_id, old_bom, new_bom} -> orders.rebundle.rebuild_bundle: the SKU's split rows are rebuilt
   from old_bom and split with new_bom (an empty new_bom leaves them plain); its plain rows are split
   bundle_change_ids: bundles whose component rows the merge combined -> orders.rebundle.run
3. MERGE only: under the SKU master lock and every channel's order lock, a last sweep of rows an order
   load wrote with B meanwhile, the remaining references to B moved to A, and m_sku B deleted

GMV and quantities are unchanged by 1; 2 checks them per batch (rebuild_bundle). The DAG then re-matches
the listings waiting for review (rematch_pending_skus), whose candidates may name B or the moved codes.
"""
import json
import logging
from contextlib import closing

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from orders import rebundle
from orders.rebundle import Lookups, rebuild_bundle
from sku.process import LOCK_KEY
from staging.common import SCHEMA, log_step

log = logging.getLogger(__name__)

BATCH_ROWS = 5000
REFERENCES = (('log_sku_match', 'resolved_sku_id'), ('log_sku_match', 'suggested_sku_id'),
              ('log_dq_issue', 'sku_id'), ('log_bundle_change', 'bundle_sku_id'),
              ('m_sku_alias', 'sku_id'), ('m_platform_sku', 'sku_id'), ('m_sku_history', 'sku_id'),
              ('m_sku_bundle', 'component_sku_id'))


def run(conf: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Process the PENDING / FAILED changes (or the given change_id), oldest first."""
    ids = sorted({int(i) for i in ([conf['change_id']] if conf.get('change_id') else []) + list(conf.get('change_ids') or [])})
    hook = PostgresHook(postgres_conn_id=conn_id)
    results = []
    with closing(hook.get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {SCHEMA}.log_sku_change SET status = 'RUNNING', message = NULL
                WHERE status IN ('PENDING', 'FAILED')
                  AND (cardinality(%s::bigint[]) = 0 OR change_id = ANY(%s::bigint[]))
                RETURNING change_id, run_id, kind, source_sku_id, target_sku_id, bundle, snapshot
            """, (ids, ids))
            changes = sorted(cur.fetchall())
        conn.commit()
        for change_id, run_id, kind, source, target, bundle, snapshot in changes:
            try:
                counts = reassign(conn, run_id, kind, source, target, bundle or {}, snapshot or {}, conn_id)
                status, message = 'DONE', None
            except Exception as err:          # one bad change doesn't stop the others; retry from the page
                conn.rollback()
                log.exception('SKU change %s failed', change_id)
                counts, status, message = {}, 'FAILED', str(err)[:1000]
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {SCHEMA}.log_sku_change
                    SET status = %s, message = %s, counts = %s::jsonb, finished_at = CASE WHEN %s = 'DONE' THEN now() END
                    WHERE change_id = %s
                """, (status, message, json.dumps(counts), status, change_id))
                cur.execute(f"""
                    UPDATE {SCHEMA}.log_etl_run SET status = %s, finished_at = now(), error_message = %s
                    WHERE run_id = %s
                """, ('SUCCESS' if status == 'DONE' else 'FAILED', message, run_id))
                if status == 'DONE':
                    log_step(cur, run_id, _next_step_seq(cur, run_id), 'reassign_sku_orders', None, 't_order_detail',
                             rows_read=counts.get('rows_read', 0), rows_inserted=counts.get('rows_moved', 0),
                             rows_rejected=counts.get('lines_mismatch', 0), message=json.dumps(counts))
            conn.commit()
            log.info('SKU change %s (%s %s -> %s): %s %s', change_id, kind, source, target, status, counts or message)
            results.append({'change_id': change_id, 'kind': kind, 'status': status, **counts})
    return {'changes': results}


def _next_step_seq(cur, run_id: int) -> int:
    cur.execute(f'SELECT coalesce(max(step_seq), 0) + 1 FROM {SCHEMA}.log_etl_step WHERE run_id = %s', (run_id,))
    return cur.fetchone()[0]


def reassign(conn, run_id: int, kind: str, source: int, target: int, bundle: dict, snapshot: dict | None = None,
             conn_id: str = 'vianalytics_db') -> dict:
    with conn.cursor() as cur:
        lookups = Lookups(cur)
    conn.commit()
    if kind == 'MERGE':
        counts = _move_merge(conn, lookups, source, target)
    else:
        counts = _move_split(conn, lookups, source, target)

    rebuild = bundle.get('rebuild')
    if rebuild:
        with conn.cursor() as cur:
            lookups = Lookups(cur)           # plain rows moved; the rebuild reads them by sku_id
        conn.commit()
        # a split moves only the split rows that belonged to the source; rows that already resolved to the
        # target were split with the target's own components and stay as they are
        split_ids = _moved_split_rows(conn, lookups, snapshot, source, target) if kind == 'SPLIT' else None
        r = rebuild_bundle(conn, lookups, run_id, int(rebuild['sku_id']), rebuild.get('old_bom'),
                           rebuild.get('new_bom') or [], split_ids=split_ids)
        counts.update({k: r[k] for k in ('lines_rebuilt', 'lines_mismatch', 'detail_rows')})
        counts['orders_touched'] = counts.get('orders_touched', 0) + r['orders_touched']
    if bundle.get('bundle_change_ids'):
        done = rebundle.run({'change_ids': bundle['bundle_change_ids']}, conn_id=conn_id)
        counts['bundles_resplit'] = sum(1 for c in done['changes'] if c['status'] == 'DONE')
        failed = [c['change_id'] for c in done['changes'] if c['status'] != 'DONE']
        if failed:
            raise RuntimeError(f'bundle re-split failed for log_bundle_change {failed}')
    if kind == 'MERGE':
        counts['rows_swept'] = _finish_merge(conn, lookups, source, target)
    return counts


class BeforeLookups:
    """Line -> SKU as it resolved before the change: the current lookups with the rows of both SKUs
    taken from the change's snapshot (aliases, listing history codes and listings)."""

    def __init__(self, lookups: Lookups, snapshot: dict):
        from sku.rules import clean_sku
        self.code, self.listing = dict(lookups.code), dict(lookups.listing)
        snap_skus = {r['sku_id'] for r in snapshot.get('m_sku') or []}
        for code, sku in list(self.code.items()):          # codes the change gave to one of the two SKUs
            if sku in snap_skus:
                del self.code[code]
        for h in sorted(snapshot.get('m_sku_history') or [], key=lambda r: r.get('version') or 0):
            for col in ('old_seller_sku', 'new_seller_sku'):
                code = clean_sku(h.get(col))
                if code:
                    self.code[code] = h['sku_id']
        for a in snapshot.get('m_sku_alias') or []:
            self.code[a['sku_code']] = a['sku_id']
        for p in snapshot.get('m_platform_sku') or []:
            self.listing[p['platform_sku_id']] = p['sku_id']

    line_sku = Lookups.line_sku


def _moved_split_rows(conn, lookups: Lookups, snapshot: dict, source: int, target: int) -> set:
    """order_detail_ids of split (bundle component) rows that resolved to the source before the change
    and resolve to the target now."""
    listings = [pid for pid, sku in lookups.listing.items() if sku == target]
    codes = [code for code, sku in lookups.code.items() if sku == target]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT order_detail_id, seller_sku, platform_sku_id FROM {SCHEMA}.t_order_detail
            WHERE is_bundle AND (platform_sku_id = ANY(%s) OR upper(btrim(seller_sku)) = ANY(%s)
                                 OR upper(regexp_replace(btrim(seller_sku), '^(TKP|TTS|LZD|SHP|BLI)[_-]', '', 'i')) = ANY(%s))
        """, (listings, codes, codes))
        rows = cur.fetchall()
    conn.commit()
    before = BeforeLookups(lookups, snapshot)
    return {int(i) for i, s, p in rows
            if before.line_sku(s, p) == source and lookups.line_sku(s, p) == target}


def _channels(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(f'SELECT channel_id, channel_code FROM {SCHEMA}.m_channel ORDER BY channel_code')
        rows = cur.fetchall()
    conn.commit()
    return dict(rows)


def _move_merge(conn, lookups: Lookups, source: int, target: int) -> dict:
    """Every row of B -> A, one channel at a time under its order lock."""
    moved = 0
    for channel_id, code in _channels(conn).items():
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (f'orders:{code}',))
            cur.execute(f"""
                UPDATE {SCHEMA}.t_order_detail d SET sku_id = %s
                FROM {SCHEMA}.t_order o
                WHERE o.order_id = d.order_id AND o.channel_id = %s AND d.sku_id = %s
            """, (target, channel_id, source))
            moved += cur.rowcount
        conn.commit()
    return {'rows_read': moved, 'rows_moved': moved}


def split_moves(rows: pd.DataFrame, lookups: Lookups, target: int) -> pd.DataFrame:
    """Plain rows of the source that now resolve to the target (seller SKU first, else the listing)."""
    if rows.empty:
        return rows
    owner = [lookups.line_sku(s, p) for s, p in zip(rows['seller_sku'], rows['platform_sku_id'])]
    return rows[pd.Series(owner, index=rows.index) == target]


def _move_split(conn, lookups: Lookups, source: int, target: int) -> dict:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT d.order_detail_id, d.seller_sku, d.platform_sku_id, o.channel_id
            FROM {SCHEMA}.t_order_detail d JOIN {SCHEMA}.t_order o USING (order_id)
            WHERE d.sku_id = %s AND NOT d.is_bundle
        """, (source,))
        rows = pd.DataFrame(cur.fetchall(), columns=['order_detail_id', 'seller_sku', 'platform_sku_id', 'channel_id'],
                            dtype=object)
    conn.commit()
    moves = split_moves(rows, lookups, target)
    channels = _channels(conn)
    moved = 0
    for channel_id, part in moves.groupby('channel_id'):
        ids = [int(i) for i in part['order_detail_id']]
        for start in range(0, len(ids), BATCH_ROWS):
            with conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (f'orders:{channels[int(channel_id)]}',))
                cur.execute(f"""
                    UPDATE {SCHEMA}.t_order_detail SET sku_id = %s
                    WHERE order_detail_id = ANY(%s::bigint[]) AND sku_id = %s
                """, (target, ids[start:start + BATCH_ROWS], source))
                moved += cur.rowcount
            conn.commit()
    return {'rows_read': len(rows), 'rows_moved': moved}


def _finish_merge(conn, lookups: Lookups, source: int, target: int) -> int:
    """Last sweep, remaining references, and the merged-away SKU deleted - under every lock that writes them."""
    with conn.cursor() as cur:
        try:
            cur.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK_KEY,))
            for code in sorted(lookups.channel.values()):
                cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (f'orders:{code}',))
            cur.execute(f'UPDATE {SCHEMA}.t_order_detail SET sku_id = %s WHERE sku_id = %s', (target, source))
            swept = cur.rowcount
            for table, column in REFERENCES:
                cur.execute(f'UPDATE {SCHEMA}.{table} SET {column} = %s WHERE {column} = %s', (target, source))
            cur.execute(f'DELETE FROM {SCHEMA}.dm_sales_sku_daily WHERE sku_id = %s', (source,))
            cur.execute(f'DELETE FROM {SCHEMA}.m_sku WHERE sku_id = %s', (source,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return swept
