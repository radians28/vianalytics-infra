"""handle_order_dag_{channel}: stg rows of an upload -> royal_canin.t_order / t_order_detail.

Port of vianalytics-svc/sku_consolidation/common/tx.py (offline step 6), reading Postgres:
1. the upload's last successful staging run (log_etl_run.upload_id) gives the stg rows (load_run_id)
2. lines are normalised per marketplace (orders.normalize); lines without order no / created time
   are rejected
3. each line is resolved to the SKU master: cleaned seller SKU via m_sku_alias / m_sku_history,
   else the listing (m_platform_sku). Lines that still have no SKU belong to listings waiting in
   log_sku_match (PENDING); they are skipped here. Once they are resolved through the SKU review
   API, handle_continue_order_data_{channel} (run_continue) loads ONLY those listings' lines: lines
   already written (including auto CREATED_NEW listings) and existing headers are never rewritten,
   a mixed order's header only gets item_line_count / total_quantity recounted
4. a bundle line (sku_id in m_sku_bundle) becomes one line per component, one level only (a component
   that is a bundle itself stays that bundle SKU): quantity x component qty, gmv_1 and discounts
   split by price_ratio, rounding remainder on the last component, line_key '<key>#<component_seq>'
5. one header per order with at least one loaded line; city_id from the postal code (m_zipcode), else
   the marketplace wording via m_city_alias (ensure_cities). Order-level amounts (gmv_2, shipping fee) come
   from every line of the order before the bundle breakdown; item_line_count / total_quantity are
   recounted from the order's t_order_detail rows, so a header matches the lines it actually has
6. upserts on (channel_id, order_no) and (order_id, line_key): ids come from the IDENTITY sequences
   (never max + 1), so a retry or an overlapping re-upload updates instead of duplicating

Concurrency: the order DAGs run one at a time per channel (max_active_runs=1) and each channel's
write transaction also takes pg_advisory_xact_lock(hashtext('orders:<CHANNEL>')). Different channels
never touch the same t_order row, so they run in parallel.
"""
import json
import logging
from contextlib import closing

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from orders import normalize as nz
from orders.city import RefCity, Reference, pick_city
from orders.config import SOURCES, stg_column_names
from orders.status import CONTINUE_RESOLUTIONS
from sku.rules import clean_sku
from staging.common import BATCH_SIZE, SCHEMA, log_step

log = logging.getLogger(__name__)

# rows per INSERT ... SELECT FROM unnest(...) statement
CHUNK = 5000


def run(upload: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Load the orders of one uploaded file (all its channels); returns the upload with counts."""
    upload_id, channel = _check_conf(upload)
    hook = PostgresHook(postgres_conn_id=conn_id)
    results = {}
    with closing(hook.get_conn()) as conn:
        run_id, channels = _start(conn, upload_id, channel)
        for code, table in SOURCES[channel]:
            results[code] = load_channel(conn, code, table, run_id, *channels[code])
            log.info('%s: %s', code, results[code])
    return {**upload, 'load_run_id': run_id, 'orders': results}


def run_continue(conf: dict, conn_id: str = 'vianalytics_db') -> dict:
    """handle_continue_order_data_{channel}: load ONLY the order lines of the upload's listings that
    were resolved after the first load skipped them: by a person in the SKU review (manual), along
    with a reviewer's decision on the same seller SKU (sibling), or by a later upload's SKU step
    (auto_mapped). A review row is shared by every upload holding the listing (log_sku_match_upload).

    conf['match_ids'] narrows the listings (svc sends them); without it every such listing of the
    upload is loaded. Lines the first load already wrote are never touched: auto_created listings,
    PENDING rows and every other listing of the upload are ignored, and existing order headers are
    kept as they are (only their item_line_count / total_quantity are recounted)."""
    upload_id, channel = _check_conf(conf)
    match_ids = sorted({int(i) for i in conf.get('match_ids') or []})

    hook = PostgresHook(postgres_conn_id=conn_id)
    results = {}
    with closing(hook.get_conn()) as conn:
        run_id, channels = _start(conn, upload_id, channel)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT m.match_id, m.channel_id, m.merchant_sku, coalesce(m.sku_code, '')
                FROM {SCHEMA}.log_sku_match m
                JOIN {SCHEMA}.log_sku_match_upload mu ON mu.match_id = m.match_id AND mu.upload_id = %s
                WHERE m.status IN ('MAPPED', 'CREATED_NEW') AND m.resolution = ANY(%s)
                  AND (cardinality(%s::bigint[]) = 0 OR m.match_id = ANY(%s::bigint[]))
            """, (upload_id, list(CONTINUE_RESOLUTIONS), match_ids, match_ids))
            rows = cur.fetchall()
        conn.commit()
        ignored = sorted(set(match_ids) - {r[0] for r in rows})
        if ignored:
            log.warning('match ids ignored (not in this upload, still PENDING or created automatically): %s',
                        ignored)
        if not rows:
            raise RuntimeError(f'No resolved SKU to load for upload {upload_id} (match_ids {match_ids or "all"})')
        for code, table in SOURCES[channel]:
            channel_id = channels[code][0]
            only = {(m, s) for _, cid, m, s in rows if cid == channel_id}
            if only:
                results[code] = load_channel(conn, code, table, run_id, *channels[code], only=only)
                log.info('%s: %s', code, results[code])
    return {**conf, 'load_run_id': run_id, 'match_ids': sorted(r[0] for r in rows), 'ignored_match_ids': ignored,
            'orders': results}


def _check_conf(conf: dict) -> tuple[str, str]:
    upload_id, channel = conf.get('upload_id'), conf.get('channel')
    if not upload_id:
        raise ValueError('Missing upload_id in the DAG run conf')
    if channel not in SOURCES:
        raise ValueError(f'Unsupported channel: {channel!r}')
    return upload_id, channel


def _start(conn, upload_id: str, channel: str) -> tuple[int, dict]:
    """The upload's staging run id, a heartbeat, and code -> (channel_id, gmv2_dedup) of its channels."""
    with conn.cursor() as cur:
        run_id = staging_run_id(cur, upload_id)
        cur.execute("""
            UPDATE upload_progress SET last_heartbeat = now(), updated_at = now()
            WHERE upload_id = %s
        """, (upload_id,))
        cur.execute(f'SELECT channel_code, channel_id, gmv2_dedup_by_order FROM {SCHEMA}.m_channel')
        channels = {code: (cid, dedup) for code, cid, dedup in cur.fetchall()}
    conn.commit()
    for code, _ in SOURCES[channel]:
        if code not in channels:
            raise RuntimeError(f'Channel {code} missing from {SCHEMA}.m_channel')
    return run_id, channels


def staging_run_id(cur, upload_id: str) -> int:
    cur.execute(f"""
        SELECT run_id FROM {SCHEMA}.log_etl_run
        WHERE upload_id = %s AND status = 'SUCCESS'
        ORDER BY run_id DESC LIMIT 1
    """, (upload_id,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f'No successful staging load for upload {upload_id}')
    return row[0]


# ---------------------------------------------------------------- per channel

def load_channel(conn, code: str, table: str, run_id: int, channel_id: int, gmv2_dedup: bool,
                 only: set | None = None) -> dict:
    """Load one channel's lines. `only` = {(merchant_sku, sku_code)} of manually resolved listings:
    then just their lines are written and existing headers are kept (run_continue)."""
    step = 'load_orders' if only is None else 'continue_orders'
    raw = read_stg(conn, code, table, run_id)
    counts = {'lines': len(raw), 'rejected': 0, 'duplicate_lines': 0, 'skipped_pending_lines': 0,
              'skipped_pending_orders': 0, 'bundle_lines': 0, 'orders': 0, 'detail_rows': 0}
    if only is not None:
        counts = {'lines': len(raw), 'rejected': 0, 'duplicate_lines': 0, 'target_lines': 0,
                  'ignored_lines': 0, 'bundle_lines': 0, 'orders': 0, 'orders_new': 0,
                  'orders_existing': 0, 'detail_rows': 0}
    if raw.empty:
        _log(conn, run_id, code, counts, step)
        return counts

    lines = nz.normalize(raw, code)
    bad = lines['created_at'].isna() | lines['order_no'].isna()
    counts['rejected'] = int(bad.sum())
    lines = lines[~bad].sort_values(['created_at', 'order_no', 'source_file', 'source_row_no'], kind='stable')
    dup = lines.duplicated(['order_no', 'line_key'], keep='last')
    if dup.any():                       # same order line twice in one file: the last one wins
        counts['duplicate_lines'] = int(dup.sum())
        lines = lines[~dup]

    with conn.cursor() as cur:
        ms = Masters(cur, channel_id)
    conn.commit()
    lines = ms.resolve(lines)

    if only is None:
        pending = lines['sku_id'].isna()
        counts['skipped_pending_lines'] = int(pending.sum())
        counts['skipped_pending_orders'] = int(lines.loc[pending, 'order_no'].nunique())
        known = lines[~pending]
    else:
        # same listing key as sku/process.py: (merchant_sku, cleaned seller SKU or '')
        keys = zip(lines['platform_sku'], lines['seller_sku'].map(lambda s: clean_sku(None if pd.isna(s) else s)))
        hit = pd.Series([(None if pd.isna(p) else p, '' if pd.isna(c) else c) in only for p, c in keys],
                        index=lines.index)
        known = lines[hit]
        counts['target_lines'], counts['ignored_lines'] = len(known), int((~hit).sum())
        unresolved = known['sku_id'].isna()
        if unresolved.any():
            raise RuntimeError(f'{code}: {int(unresolved.sum())} lines of resolved listings still have no SKU '
                               f'(e.g. {known.loc[unresolved, "platform_sku"].iloc[0]!r})')
    counts['bundle_lines'] = int(known['is_bundle'].sum())

    detail = explode_bundles(known, ms.bom)
    if abs(detail['gmv_1'].sum() - known['gmv_1'].sum()) >= 1.0:
        raise RuntimeError(f'{code}: gmv_1 changed by the bundle breakdown')
    if detail.duplicated(['order_no', 'line_key']).any():
        raise RuntimeError(f'{code}: duplicate (order_no, line_key) after the bundle breakdown')
    detail = detail.sort_values(['order_no', 'line_key'], kind='stable')

    orders = build_orders(lines, detail, gmv2_dedup)
    counts['orders'], counts['detail_rows'] = len(orders), len(detail)
    if orders.empty:
        _log(conn, run_id, code, counts, step)
        return counts

    orders['city_id'] = ensure_cities(conn, orders)

    try:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (f'orders:{code}',))
            if only is None:
                order_id = upsert_orders(cur, orders, channel_id, run_id)
            else:
                order_id, new = insert_new_orders(cur, orders, channel_id, run_id)
                counts['orders_new'], counts['orders_existing'] = new, len(orders) - new
            detail['order_id'] = detail['order_no'].map(order_id)
            drop_stale_lines(cur, detail)
            detail_id = upsert_details(cur, detail, run_id)
            write_dq(cur, detail, detail_id)
            recount(cur, list(order_id.values()))
            log_step(cur, run_id, next_step_seq(cur, run_id), step, code, 't_order_detail',
                     rows_read=counts['lines'], rows_inserted=counts['detail_rows'],
                     rows_rejected=counts['rejected'], message=json.dumps(counts))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return counts


def _log(conn, run_id: int, code: str, counts: dict, step: str = 'load_orders'):
    with conn.cursor() as cur:
        log_step(cur, run_id, next_step_seq(cur, run_id), step, code, 't_order_detail',
                 rows_read=counts['lines'], rows_inserted=0, rows_rejected=counts['rejected'],
                 message=json.dumps(counts))
    conn.commit()


def next_step_seq(cur, run_id: int) -> int:
    cur.execute(f'SELECT coalesce(max(step_seq), 0) + 1 FROM {SCHEMA}.log_etl_step WHERE run_id = %s', (run_id,))
    return cur.fetchone()[0]


def read_stg(conn, code: str, table: str, run_id: int) -> pd.DataFrame:
    """The staging run's rows of `table`, only the columns the order load uses."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (SCHEMA, table))
        existing = {r[0] for r in cur.fetchall()}
    wanted = stg_column_names(code)
    missing = [c for c in wanted if c not in existing]
    if missing:
        log.warning('%s: columns not in %s, read as NULL: %s', code, table, missing)
    cols = [c for c in wanted if c in existing] + ['source_file', 'source_row_no']
    rows = []
    with conn.cursor(name=f'orders_{table}') as stream:
        stream.execute(f"""
            SELECT {', '.join(cols)} FROM {SCHEMA}.{table}
            WHERE load_run_id = %s ORDER BY stg_id
        """, (run_id,))
        while batch := stream.fetchmany(BATCH_SIZE):
            rows.extend(batch)
    conn.commit()                       # close the named cursor's transaction
    return pd.DataFrame(rows, columns=cols, dtype=object)


# ---------------------------------------------------------------- SKU master

class Masters:
    """The SKU lookups an order line resolves through, read once per channel."""

    def __init__(self, cur, channel_id: int):
        cur.execute(f'SELECT merchant_sku, platform_sku_id, sku_id FROM {SCHEMA}.m_platform_sku '
                    'WHERE channel_id = %s', (channel_id,))
        rows = cur.fetchall()
        self.platform_id = {r[0]: r[1] for r in rows}
        self.platform_sku = {r[0]: r[2] for r in rows}
        # seller SKU codes: m_sku_history first, m_sku_alias wins (as sku/process.py Master.sku_of_code)
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
        cur.execute(f"""
            SELECT bundle_sku_id, component_seq, component_sku_id, qty, price_ratio, coalesce(dq_flag, '')
            FROM {SCHEMA}.m_sku_bundle ORDER BY bundle_sku_id, component_seq
        """)
        bom = pd.DataFrame(cur.fetchall(), columns=['bundle_sku_id', 'component_seq', 'component_sku_id',
                                                    'qty', 'weight', 'dq_flag'])
        bom['bundle_sku_id'] = bom['bundle_sku_id'].astype('int64')
        bom['component_seq'] = bom['component_seq'].astype('int64')
        bom['component_sku_id'] = pd.to_numeric(bom['component_sku_id']).astype('Int64')
        bom['qty'] = bom['qty'].astype('int64')
        bom['weight'] = pd.to_numeric(bom['weight'], errors='coerce').astype('float64')
        bom['equal_split'] = bom['dq_flag'].str.contains('equal_split')
        self.bom = bom.drop(columns='dq_flag')
        self.bundle_ids = set(self.bom['bundle_sku_id'])

    def resolve(self, lines: pd.DataFrame) -> pd.DataFrame:
        """sku_id: cleaned seller SKU first, else the listing's SKU (offline tx.resolve)."""
        lines = lines.copy()
        lines['platform_sku_id'] = lines['platform_sku'].map(self.platform_id).astype('Int64')
        by_code = lines['seller_sku'].map(lambda s: clean_sku(None if pd.isna(s) else s)).map(self.code)
        by_listing = lines['platform_sku'].map(self.platform_sku)
        lines['sku_id'] = pd.to_numeric(by_code).fillna(pd.to_numeric(by_listing)).astype('Int64')
        lines['is_bundle'] = lines['sku_id'].isin(self.bundle_ids)
        return lines


# ---------------------------------------------------------------- bundles / headers

def explode_bundles(lines: pd.DataFrame, bom: pd.DataFrame) -> pd.DataFrame:
    """Replace each bundle line by one line per component (one level, a single join to m_sku_bundle)."""
    plain = lines[~lines['is_bundle']].copy()
    plain['dq_flag'] = None
    b = lines[lines['is_bundle']].copy()
    if b.empty:
        return plain
    b['_line'] = np.arange(len(b))
    e = b.merge(bom, left_on='sku_id', right_on='bundle_sku_id', how='inner')

    # price_ratio share; a bundle without usable ratios is split by quantity (dq equal_split)
    wsum = e.groupby('_line')['weight'].transform('sum')
    by_qty = ~(wsum > 0)
    e['share'] = np.where(by_qty, e['qty'] / e.groupby('_line')['qty'].transform('sum'), e['weight'] / wsum)
    e['equal_split'] = e['equal_split'] | by_qty
    e['is_last'] = e['component_seq'] == e.groupby('_line')['component_seq'].transform('max')

    e['line_key'] = e['line_key'] + '#' + e['component_seq'].astype(str)
    e['quantity'] = e['quantity'] * e['qty']
    e['return_quantity'] = e['return_quantity'] * e['qty']
    for c in ('gmv_1', 'seller_discount', 'platform_discount'):
        part = (e[c] * e['share']).round(2)
        rest = e[c] - part.groupby(e['_line']).transform('sum')     # rounding remainder -> last component
        e[c] = (part + rest.where(e['is_last'], 0)).round(2)
    e['unit_price'] = (e['gmv_1'] / e['quantity'].where(e['quantity'] > 0)).round(2)
    e['sku_id'] = e['component_sku_id']                 # platform_sku_id stays the bundle's listing
    e['dq_flag'] = np.where(e['equal_split'], 'equal_split',
                            np.where(e['component_sku_id'].isna(), 'component_not_in_master', None))
    e = e.drop(columns=['bundle_sku_id', 'component_seq', 'component_sku_id', 'qty', 'weight',
                        'equal_split', 'is_last', 'share', '_line'])
    return pd.concat([plain, e], ignore_index=True)


def build_orders(lines: pd.DataFrame, detail: pd.DataFrame, gmv2_dedup: bool) -> pd.DataFrame:
    """One header per order that has loaded lines; amounts from all of the order's lines (pre-breakdown)."""
    src = lines[lines['order_no'].isin(set(detail['order_no']))]
    g = src.groupby('order_no', sort=True)

    def amount(c):
        return g[c].first() if gmv2_dedup else g[c].sum(min_count=1)

    d = detail.groupby('order_no')
    o = pd.DataFrame({
        'order_created_at': g['created_at'].min(),
        'order_status': g['status'].first(), 'order_paid_at': g['paid_at'].min(),
        'order_shipped_at': g['shipped_at'].min(), 'order_completed_at': g['completed_at'].max(),
        'buyer_username': g['buyer'].first(), 'warehouse_name': g['warehouse'].first(),
        'delivery_option': g['delivery_option'].first(), 'payment_method': g['payment_method'].first(),
        'city_name': g['city'].first(), 'province_name': g['province'].first(),
        'zip_code': g['zip_code'].first(),
        'item_line_count': d.size(), 'total_quantity': d['quantity'].sum(),
        'shipping_fee': amount('shipping_fee'), 'gmv_2': amount('order_total'),
    })
    return o.rename_axis('order_no').reset_index()


def load_reference(cur) -> Reference:
    """The canonical kabupaten/kota (m_city rows with a Kemendagri city_code)."""
    cur.execute(f"""
        SELECT city_id, city_code, city_name, city_type, province_code, province_name
        FROM {SCHEMA}.m_city WHERE city_code IS NOT NULL
    """)
    return Reference(RefCity(*r) for r in cur.fetchall())


def ensure_cities(conn, orders: pd.DataFrame) -> pd.Series:
    """city_id per header, in this order:
    1. the order's postal code in m_zipcode (one kabupaten/kota per code), unless the reference lists the
       code in several cities and the wording names one of the others (orders.city.pick_city)
    2. the marketplace wording (city, province) already in m_city_alias
    3. new wording: orders.city.match() against the canonical m_city rows, saved as a new alias;
       wording that matches nothing gets its own m_city row (dq_flag 'unresolved') + alias, so it
       shows up for review instead of failing the load
    The writes (step 3) run in their own short transaction."""
    city = orders['city_name']
    province = orders['province_name'].fillna('')
    zips = orders['zip_code'] if 'zip_code' in orders else pd.Series(pd.NA, index=orders.index)
    pairs = sorted({(c, p) for c, p in zip(city, province) if not pd.isna(c)})
    zip_list = sorted({z for z in zips if not pd.isna(z)})
    try:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT zip_code, city_id, other_city_ids FROM {SCHEMA}.m_zipcode
                            WHERE zip_code = ANY(%s::text[])""", (zip_list,))
            by_zip = {z: (cid, other) for z, cid, other in cur.fetchall()}
            alias = _read_aliases(cur, pairs)
            new = [p for p in pairs if p not in alias]
            if new:
                ref = load_reference(cur)
                rows = []
                for c, p in new:
                    m = ref.match(c, p)
                    if m.city is None:
                        cur.execute(f"""
                            INSERT INTO {SCHEMA}.m_city (city_name, province_name, dq_flag)
                            VALUES (%s, %s, 'unresolved')
                            ON CONFLICT (city_name, province_name) DO NOTHING
                        """, (c, p))
                        cur.execute(f"SELECT city_id FROM {SCHEMA}.m_city WHERE city_name = %s AND province_name = %s",
                                    (c, p))
                        rows.append((c, p, cur.fetchone()[0], 'unresolved', m.flag))
                    else:
                        rows.append((c, p, m.city.city_id, m.method, m.flag))
                cur.execute(f"""
                    INSERT INTO {SCHEMA}.m_city_alias (raw_city_name, raw_province_name, city_id, match_method, dq_flag)
                    SELECT * FROM unnest(%s::text[], %s::text[], %s::int[], %s::text[], %s::text[])
                    ON CONFLICT (raw_city_name, raw_province_name) DO NOTHING
                """, [list(col) for col in zip(*rows)])
                unresolved = sum(r[3] == 'unresolved' for r in rows)
                log.info('m_city_alias: %d new wording(s), %d unresolved', len(rows), unresolved)
                alias = _read_aliases(cur, pairs)       # another load may have won the insert race
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    out = []
    for c, p, z in zip(city, province, zips):
        zip_city, other = by_zip.get(None if pd.isna(z) else z, (None, None))
        out.append(pick_city(zip_city, other, None if pd.isna(c) else alias.get((c, p))))
    return pd.Series(out, index=orders.index, dtype='Int64')


def _read_aliases(cur, pairs) -> dict:
    if not pairs:
        return {}
    cur.execute(f"""
        SELECT a.raw_city_name, a.raw_province_name, a.city_id
        FROM {SCHEMA}.m_city_alias a JOIN unnest(%s::text[], %s::text[]) AS k(raw_city_name, raw_province_name)
        USING (raw_city_name, raw_province_name)
    """, ([p[0] for p in pairs], [p[1] for p in pairs]))
    return {(r[0], r[1]): r[2] for r in cur.fetchall()}


# ---------------------------------------------------------------- writes

def _values(s: pd.Series, kind: str) -> list:
    """Series -> python values psycopg2 adapts (no numpy scalars, NaN/NaT -> None)."""
    if kind == 'ts':
        return [None if pd.isna(v) else v.to_pydatetime() for v in s]
    if kind == 'int':
        return [None if pd.isna(v) else int(v) for v in s]
    if kind == 'num':
        return [None if pd.isna(v) else round(float(v), 2) for v in s]
    if kind == 'bool':
        return [bool(v) for v in s]
    return [None if pd.isna(v) else str(v) for v in s]


def _chunks(df: pd.DataFrame):
    for start in range(0, len(df), CHUNK):
        yield df.iloc[start:start + CHUNK]


ORDER_COLS = [('order_no', 'text', 'str'), ('city_id', 'integer', 'int'), ('order_status', 'text', 'str'),
              ('order_created_at', 'timestamptz', 'ts'), ('order_paid_at', 'timestamptz', 'ts'),
              ('order_shipped_at', 'timestamptz', 'ts'), ('order_completed_at', 'timestamptz', 'ts'),
              ('buyer_username', 'text', 'str'), ('warehouse_name', 'text', 'str'),
              ('delivery_option', 'text', 'str'), ('payment_method', 'text', 'str'),
              ('item_line_count', 'integer', 'int'), ('total_quantity', 'integer', 'int'),
              ('shipping_fee', 'numeric', 'num'), ('gmv_2', 'numeric', 'num'), ('zip_code', 'text', 'str')]


def upsert_orders(cur, orders: pd.DataFrame, channel_id: int, run_id: int) -> dict:
    """order_no -> order_id of every header written."""
    names = [c for c, _, _ in ORDER_COLS]
    sql = f"""
        INSERT INTO {SCHEMA}.t_order (channel_id, {', '.join(names)}, etl_run_id)
        SELECT %s, {', '.join('u.' + c for c in names)}, %s
        FROM unnest({', '.join(f'%s::{t}[]' for _, t, _ in ORDER_COLS)}) AS u({', '.join(names)})
        ON CONFLICT (channel_id, order_no) DO UPDATE SET
            {', '.join(f'{c} = EXCLUDED.{c}' for c in names if c != 'order_no')},
            etl_run_id = EXCLUDED.etl_run_id
        RETURNING order_no, order_id
    """
    ids = {}
    for part in _chunks(orders):
        cur.execute(sql, [channel_id, run_id, *[_values(part[c], k) for c, _, k in ORDER_COLS]])
        ids.update(dict(cur.fetchall()))
    return ids


def insert_new_orders(cur, orders: pd.DataFrame, channel_id: int, run_id: int) -> tuple[dict, int]:
    """Headers for orders that have none yet; an existing header is left exactly as it is.
    Returns order_no -> order_id of every order in `orders` and the number of new headers."""
    names = [c for c, _, _ in ORDER_COLS]
    ids, new = {}, 0
    for part in _chunks(orders):
        cur.execute(f"""
            INSERT INTO {SCHEMA}.t_order (channel_id, {', '.join(names)}, etl_run_id)
            SELECT %s, {', '.join('u.' + c for c in names)}, %s
            FROM unnest({', '.join(f'%s::{t}[]' for _, t, _ in ORDER_COLS)}) AS u({', '.join(names)})
            ON CONFLICT (channel_id, order_no) DO NOTHING
            RETURNING order_no, order_id
        """, [channel_id, run_id, *[_values(part[c], k) for c, _, k in ORDER_COLS]])
        inserted = dict(cur.fetchall())
        new += len(inserted)
        ids.update(inserted)
        cur.execute(f"""
            SELECT order_no, order_id FROM {SCHEMA}.t_order
            WHERE channel_id = %s AND order_no = ANY(%s::text[])
        """, (channel_id, _values(part['order_no'], 'str')))
        ids.update(dict(cur.fetchall()))
    return ids, new


def drop_stale_lines(cur, detail: pd.DataFrame):
    """Rows of a re-loaded line that this load no longer produces (e.g. a bundle's components changed)."""
    base = detail['line_key'].str.split('#', n=1).str[0]
    keys = pd.DataFrame({'order_id': detail['order_id'], 'base': base}).drop_duplicates()
    cur.execute(f"""
        DELETE FROM {SCHEMA}.t_order_detail d
        WHERE (d.order_id, split_part(d.line_key, '#', 1)) IN (
                SELECT * FROM unnest(%s::bigint[], %s::text[]))
          AND (d.order_id, d.line_key) NOT IN (
                SELECT * FROM unnest(%s::bigint[], %s::text[]))
    """, (_values(keys['order_id'], 'int'), _values(keys['base'], 'str'),
          _values(detail['order_id'], 'int'), _values(detail['line_key'], 'str')))


DETAIL_COLS = [('order_id', 'bigint', 'int'), ('line_key', 'text', 'str'), ('platform_sku_id', 'integer', 'int'),
               ('sku_id', 'integer', 'int'), ('seller_sku', 'text', 'str'), ('product_name', 'text', 'str'),
               ('item_status', 'text', 'str'), ('quantity', 'integer', 'int'),
               ('return_quantity', 'integer', 'int'), ('unit_price', 'numeric', 'num'),
               ('seller_discount', 'numeric', 'num'), ('platform_discount', 'numeric', 'num'),
               ('gmv_1', 'numeric', 'num'), ('unit_sold', 'integer', 'int'), ('is_bundle', 'boolean', 'bool')]


def upsert_details(cur, detail: pd.DataFrame, run_id: int) -> dict:
    """(order_id, line_key) -> order_detail_id of every line written."""
    detail = detail.assign(unit_sold=detail['quantity'])
    names = [c for c, _, _ in DETAIL_COLS]
    sql = f"""
        INSERT INTO {SCHEMA}.t_order_detail ({', '.join(names)}, etl_run_id)
        SELECT {', '.join('u.' + c for c in names)}, %s
        FROM unnest({', '.join(f'%s::{t}[]' for _, t, _ in DETAIL_COLS)}) AS u({', '.join(names)})
        ON CONFLICT (order_id, line_key) DO UPDATE SET
            {', '.join(f'{c} = EXCLUDED.{c}' for c in names if c not in ('order_id', 'line_key'))},
            etl_run_id = EXCLUDED.etl_run_id
        RETURNING order_id, line_key, order_detail_id
    """
    ids = {}
    for part in _chunks(detail):
        cur.execute(sql, [run_id, *[_values(part[c], k) for c, _, k in DETAIL_COLS]])
        ids.update({(r[0], r[1]): r[2] for r in cur.fetchall()})
    return ids


def write_dq(cur, detail: pd.DataFrame, detail_id: dict):
    """t_order_detail_dq for the written lines: cleared, then the current flags inserted."""
    all_ids = list(detail_id.values())
    cur.execute(f'DELETE FROM {SCHEMA}.t_order_detail_dq WHERE order_detail_id = ANY(%s::bigint[])', (all_ids,))
    flagged = detail[detail['dq_flag'].notna()]
    if flagged.empty:
        return
    ids = [detail_id[(int(o), k)] for o, k in zip(flagged['order_id'], flagged['line_key'])]
    cur.execute(f"""
        INSERT INTO {SCHEMA}.t_order_detail_dq (order_detail_id, dq_flag)
        SELECT * FROM unnest(%s::bigint[], %s::text[])
    """, (ids, _values(flagged['dq_flag'], 'str')))


def recount(cur, order_ids: list):
    """Header line count / quantity from the order's stored lines (covers lines loaded by other runs)."""
    cur.execute(f"""
        UPDATE {SCHEMA}.t_order o
        SET item_line_count = d.n, total_quantity = d.q
        FROM (SELECT order_id, count(*) AS n, sum(quantity) AS q
              FROM {SCHEMA}.t_order_detail WHERE order_id = ANY(%s::bigint[])
              GROUP BY order_id) d
        WHERE o.order_id = d.order_id
          AND (o.item_line_count IS DISTINCT FROM d.n OR o.total_quantity IS DISTINCT FROM d.q)
    """, ([int(i) for i in order_ids],))
