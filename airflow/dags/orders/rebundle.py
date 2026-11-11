"""reconsolidate_bundle_orders: split stored order lines again after a bundle's components changed.

The order load splits a bundle line into one t_order_detail row per component (orders/load.py
explode_bundles) and keeps only those rows: line_key '<base>#<seq>', quantity = line qty x component
qty, gmv_1 / discounts shared by price_ratio, sku_id = component, is_bundle = true, platform_sku_id and
seller_sku still the bundle listing's. A SKU bundling upload (svc) replaces m_sku_bundle and records
the old and new component lists in royal_canin.log_bundle_change. For each change:

1. find the stored lines of the bundle: split rows whose line resolves to it (cleaned seller SKU via
   m_sku_alias / m_sku_history first, else the listing, as the order load resolves a line), and
   unsplit rows of the SKU (it was not a bundle when they were loaded)
2. rebuild the original line of every split group from the OLD list: quantity = row quantity / old
   component qty (the same for every component), return_quantity alike, gmv_1 and discounts summed.
   A group that doesn't add up (missing component, uneven quantities) is left as it is and flagged
   rebundle_mismatch in t_order_detail_dq
3. split the original again with the NEW list (explode_bundles), replace the group's rows
   (drop_stale_lines + upsert_details, one transaction per batch under the channel's order lock) and
   recount the touched order headers (gmv_2 / shipping fee are order level and stay as they are)
"""
import json
import logging
from contextlib import closing

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from orders.load import drop_stale_lines, explode_bundles, recount, upsert_details, write_dq
from sku.rules import clean_sku
from staging.common import SCHEMA, log_step

log = logging.getLogger(__name__)

BATCH_GROUPS = 2000          # original lines per write transaction
MISMATCH = 'rebundle_mismatch'


def run(conf: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Process the PENDING (or the given) log_bundle_change rows, oldest first."""
    change_ids = sorted({int(i) for i in conf.get('change_ids') or []})
    hook = PostgresHook(postgres_conn_id=conn_id)
    results = []
    with closing(hook.get_conn()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {SCHEMA}.log_bundle_change SET status = 'RUNNING', message = NULL
                WHERE status IN ('PENDING', 'FAILED')
                  AND (cardinality(%s::bigint[]) = 0 OR change_id = ANY(%s::bigint[]))
                RETURNING change_id, run_id, bundle_sku_id, old_bom, new_bom
            """, (change_ids, change_ids))
            changes = sorted(cur.fetchall())
            lookups = Lookups(cur) if changes else None
        conn.commit()
        for change_id, run_id, bundle_sku_id, old_bom, new_bom in changes:
            try:
                counts = rebuild_bundle(conn, lookups, run_id, bundle_sku_id, old_bom, new_bom)
                status, message = 'DONE', json.dumps(counts)
            except Exception as err:     # one bad bundle doesn't stop the others
                conn.rollback()
                log.exception('change %s (bundle %s) failed', change_id, bundle_sku_id)
                counts, status, message = {}, 'FAILED', str(err)[:1000]
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {SCHEMA}.log_bundle_change
                    SET status = %s, message = %s, processed_at = now(),
                        lines_rebuilt = %s, lines_mismatch = %s, orders_touched = %s
                    WHERE change_id = %s
                """, (status, message, counts.get('lines_rebuilt'), counts.get('lines_mismatch'),
                      counts.get('orders_touched'), change_id))
                if status == 'DONE':
                    log_step(cur, run_id, _next_step_seq(cur, run_id), 'rebundle_orders', None, 't_order_detail',
                             rows_read=counts['rows_read'], rows_inserted=counts['detail_rows'],
                             rows_rejected=counts['lines_mismatch'], message=message)
            conn.commit()
            results.append({'change_id': change_id, 'bundle_sku_id': bundle_sku_id, 'status': status, **counts})
            log.info('change %s: %s %s', change_id, status, message)
    return {'changes': results}


def _next_step_seq(cur, run_id: int) -> int:
    cur.execute(f'SELECT coalesce(max(step_seq), 0) + 1 FROM {SCHEMA}.log_etl_step WHERE run_id = %s', (run_id,))
    return cur.fetchone()[0]


class Lookups:
    """How a stored line maps back to its SKU (as orders.load.Masters resolves it), and channel codes."""

    def __init__(self, cur):
        cur.execute(f'SELECT platform_sku_id, sku_id FROM {SCHEMA}.m_platform_sku')
        self.listing = dict(cur.fetchall())
        self.code = {}
        cur.execute(f"""
            SELECT seller_sku, sku_id FROM (
                SELECT old_seller_sku AS seller_sku, sku_id, version FROM {SCHEMA}.m_sku_history
                UNION ALL
                SELECT new_seller_sku, sku_id, version FROM {SCHEMA}.m_sku_history) h
            WHERE seller_sku IS NOT NULL ORDER BY version
        """)
        for seller_sku, sku_id in cur.fetchall():
            code = clean_sku(seller_sku)
            if code:
                self.code[code] = sku_id
        cur.execute(f'SELECT sku_code, sku_id FROM {SCHEMA}.m_sku_alias')
        self.code.update(dict(cur.fetchall()))
        cur.execute(f'SELECT channel_id, channel_code FROM {SCHEMA}.m_channel')
        self.channel = dict(cur.fetchall())

    def line_sku(self, seller_sku, platform_sku_id):
        code = clean_sku(None if seller_sku is None or pd.isna(seller_sku) else seller_sku)
        sku_id = self.code.get(code) if code else None
        if sku_id is None and platform_sku_id is not None and not pd.isna(platform_sku_id):
            sku_id = self.listing.get(int(platform_sku_id))
        return sku_id


DETAIL_READ = ['order_detail_id', 'order_id', 'line_key', 'platform_sku_id', 'sku_id', 'seller_sku', 'product_name',
               'item_status', 'quantity', 'return_quantity', 'unit_price', 'seller_discount', 'platform_discount',
               'gmv_1', 'is_bundle']


def _bom_frame(bundle_sku_id: int, bom: list[dict]) -> pd.DataFrame:
    """The new component list in the shape explode_bundles expects (orders.load.Masters.bom)."""
    rows = [{'bundle_sku_id': bundle_sku_id, 'component_seq': int(c['seq']),
             'component_sku_id': c['component_sku_id'], 'qty': int(c['qty']),
             'weight': float(c['price_ratio']) if c.get('price_ratio') is not None else np.nan,
             'equal_split': 'equal_split' in (c.get('dq_flag') or '')} for c in bom]
    df = pd.DataFrame(rows)
    df['component_sku_id'] = pd.to_numeric(df['component_sku_id']).astype('Int64')
    return df


def _read_lines(conn, lookups: Lookups, bundle_sku_id: int) -> pd.DataFrame:
    """Stored rows of the bundle: split rows that resolve to it, and its unsplit rows."""
    listings = [pid for pid, sku in lookups.listing.items() if sku == bundle_sku_id]
    codes = [code for code, sku in lookups.code.items() if sku == bundle_sku_id]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT {', '.join('d.' + c for c in DETAIL_READ)}, o.channel_id
            FROM {SCHEMA}.t_order_detail d JOIN {SCHEMA}.t_order o USING (order_id)
            WHERE (d.is_bundle AND (d.platform_sku_id = ANY(%s) OR upper(btrim(d.seller_sku)) = ANY(%s)))
               OR (NOT d.is_bundle AND d.sku_id = %s)
        """, (listings, codes, bundle_sku_id))
        rows = cur.fetchall()
    conn.commit()
    df = pd.DataFrame(rows, columns=DETAIL_READ + ['channel_id'], dtype=object)
    if df.empty:
        return df
    split = df['is_bundle'].astype(bool)
    owner = [lookups.line_sku(s, p) for s, p in zip(df['seller_sku'], df['platform_sku_id'])]
    keep = ~split | (pd.Series(owner, index=df.index) == bundle_sku_id)
    return df[keep].reset_index(drop=True)


def _originals(df: pd.DataFrame, bundle_sku_id: int, old_bom: list[dict] | None):
    """(original lines, detail ids of groups that don't add up) from the stored rows."""
    num = ['quantity', 'return_quantity', 'unit_price', 'seller_discount', 'platform_discount', 'gmv_1']
    df = df.copy()
    for c in num:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df['base'] = df['line_key'].str.split('#', n=1).str[0]
    df['seq'] = pd.to_numeric(df['line_key'].str.split('#', n=1).str[1], errors='coerce')

    plain = df[~df['is_bundle'].astype(bool)].copy()       # never split: already the original line
    old_qty = {int(c['seq']): int(c['qty']) for c in (old_bom or [])}
    originals, mismatch = [plain], []
    split = df[df['is_bundle'].astype(bool)]
    for (_, _), g in split.groupby(['order_id', 'base'], sort=False):
        seqs = set(g['seq'].dropna().astype(int))
        qty = {int(s): q for s, q in zip(g['seq'], g['quantity']) if not pd.isna(s)}
        ok = old_qty and seqs == set(old_qty) and g['seq'].notna().all()
        if ok:
            per_unit = {qty[s] / old_qty[s] for s in seqs}
            ok = len(per_unit) == 1 and float(next(iter(per_unit))).is_integer()
        if not ok:
            mismatch.extend(int(i) for i in g['order_detail_id'])
            continue
        first = g.sort_values('seq').iloc[0]
        line_qty = int(next(iter(per_unit)))
        ret = first['return_quantity'] / old_qty[int(first['seq'])] if not pd.isna(first['return_quantity']) else 0
        gmv = g['gmv_1'].sum(min_count=1)
        originals.append(pd.DataFrame([{
            **{c: first[c] for c in ('order_id', 'platform_sku_id', 'seller_sku', 'product_name', 'item_status',
                                     'channel_id')},
            'line_key': first['base'], 'base': first['base'], 'sku_id': bundle_sku_id, 'is_bundle': True,
            'quantity': line_qty, 'return_quantity': int(round(ret)),
            'gmv_1': gmv, 'seller_discount': g['seller_discount'].sum(min_count=1),
            'platform_discount': g['platform_discount'].sum(min_count=1),
            'unit_price': round(gmv / line_qty, 2) if line_qty and not pd.isna(gmv) else np.nan,
        }]))
    lines = pd.concat(originals, ignore_index=True)
    lines['sku_id'] = bundle_sku_id
    lines['is_bundle'] = True
    return lines, mismatch


def _plain(lines: pd.DataFrame) -> pd.DataFrame:
    """Rebuilt original lines written as they are, not split (no component list to split them with)."""
    plain = lines.drop(columns=['base']).copy()
    plain['is_bundle'] = False
    plain['dq_flag'] = None
    return plain


def rebuild_bundle(conn, lookups: Lookups, run_id: int, bundle_sku_id: int, old_bom, new_bom,
                   split_ids: set | None = None) -> dict:
    """Split the stored lines of `bundle_sku_id` again: rebuilt from `old_bom`, split with `new_bom`
    (an empty `new_bom` keeps the rebuilt lines plain). `split_ids` limits the split rows taken to those
    order_detail_ids - the ones that were split with `old_bom` (a SKU split moves only some of them)."""
    df = _read_lines(conn, lookups, bundle_sku_id)
    if split_ids is not None and not df.empty:
        df = df[~df['is_bundle'].astype(bool) | df['order_detail_id'].isin(split_ids)].reset_index(drop=True)
    counts = {'rows_read': len(df), 'lines_rebuilt': 0, 'lines_mismatch': 0, 'orders_touched': 0, 'detail_rows': 0}
    if df.empty:
        return counts
    lines, mismatch = _originals(df, bundle_sku_id, old_bom)
    # no components (any more): the rebuilt originals stay plain lines of the SKU (a SKU split or merged
    # onto a SKU that is not a bundle)
    bom = _bom_frame(bundle_sku_id, new_bom) if new_bom else None

    for channel_id, part in lines.groupby('channel_id'):
        code = lookups.channel.get(int(channel_id), str(channel_id))
        keys = part[['order_id', 'base']].drop_duplicates()
        for start in range(0, len(keys), BATCH_GROUPS):
            batch = part.merge(keys.iloc[start:start + BATCH_GROUPS], on=['order_id', 'base'])
            detail = explode_bundles(batch.drop(columns=['base']), bom) if bom is not None else _plain(batch)
            if abs(pd.to_numeric(detail['gmv_1']).sum() - pd.to_numeric(batch['gmv_1']).sum()) >= 1.0:
                raise RuntimeError(f'bundle {bundle_sku_id}: gmv_1 changed by the new breakdown')
            detail = detail.sort_values(['order_id', 'line_key'], kind='stable')
            try:
                with conn.cursor() as cur:
                    cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (f'orders:{code}',))
                    drop_stale_lines(cur, detail)
                    detail_id = upsert_details(cur, detail, run_id)
                    write_dq(cur, detail, detail_id)
                    order_ids = sorted({int(i) for i in detail['order_id']})
                    recount(cur, order_ids)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            counts['lines_rebuilt'] += len(batch)
            counts['detail_rows'] += len(detail)
            counts['orders_touched'] += len(order_ids)

    if mismatch:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {SCHEMA}.t_order_detail_dq (order_detail_id, dq_flag)
                SELECT unnest(%s::bigint[]), %s
                ON CONFLICT (order_detail_id) DO UPDATE SET dq_flag = EXCLUDED.dq_flag
            """, (mismatch, MISMATCH))
        conn.commit()
        counts['lines_mismatch'] = len({i for i in mismatch})
    return counts
