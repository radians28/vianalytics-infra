"""One-off cleanup of royal_canin.m_city: marketplace wording -> canonical Kemendagri kabupaten/kota.

    python airflow/scripts/city_cleanup.py [--dsn URL] [--out DIR]           # dry run: review workbook only
    python airflow/scripts/city_cleanup.py --rehearse                        # run every write, then roll back
    python airflow/scripts/city_cleanup.py --apply [--dsn URL] [--out DIR]   # write (after review)

Reference: vianalytics-svc/sku_consolidation/reference/ref_city.csv and ref_zipcode.csv (see its README).
Needs the svc migration a1c4e7f20b10 (m_city.city_code, m_city_alias, m_zipcode, t_order.zip_code).

1. every m_city row without a city_code (raw wording) is matched by name (orders.city.match)
2. zip evidence: the postal code of each order (Shopee / Blibli address, Lazada shipping_post_code,
   TikTok / Tokopedia zipcode when not masked) -> reference city. Only a wording the name cannot match
   takes the zip majority (>= ZIP_SHARE of its orders with a known zip, at least ZIP_MIN orders;
   zip_majority). A name match is never overridden: regencies split off later (Tulang Bawang Barat,
   Mamuju Tengah, ...) share postal codes with their parent; a disagreement is flagged name_zip_conflict
3. every order: the city of its own postal code, unless the reference lists that code in several cities
   and the wording names one of the others (orders.city.pick_city); no known postal code -> the wording
4. --apply, one transaction under the orders advisory locks of all channels: canonical rows upserted into
   m_city (an existing raw row with exactly the canonical name is reused), m_zipcode filled, every raw
   wording into m_city_alias, t_order.city_id / zip_code and dm_sales_daily.city_id repointed, resolved raw
   rows deleted, unresolved raw rows kept with dq_flag 'unresolved'. A CSV backup of m_city and
   t_order(order_id, city_id, zip_code) is written to --out first.
"""
import argparse
import csv
import io
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'airflow' / 'dags'))
from orders.city import Reference, RefCity, extract_zip, pick_city  # noqa: E402
from orders.config import STG_MAP  # noqa: E402

SCHEMA = 'royal_canin'
REF_DIR = ROOT / 'vianalytics-svc' / 'sku_consolidation' / 'reference'
SOURCE = 'kepmendagri_2025'
STG = {'SHOPEE': 'stg_shopee', 'TOKOPEDIA': 'stg_tokopedia', 'TIKTOK': 'stg_tiktok',
       'LAZADA': 'stg_lazada', 'BLIBLI': 'stg_blibli'}
ZIP_SHARE = 0.8
ZIP_MIN = 5
DEFAULT_DSN = os.environ.get('VIANALYTICS_DSN', 'postgresql://postgres:mysecretpassword@localhost:15432/vianalytics')


def read_reference():
    with open(REF_DIR / 'ref_city.csv', encoding='utf-8') as f:
        cities = [RefCity(None, r['city_code'], r['city_name'], r['city_type'], r['province_code'], r['province_name'])
                  for r in csv.DictReader(f)]
    with open(REF_DIR / 'ref_zipcode.csv', encoding='utf-8') as f:
        zips = list(csv.DictReader(f))
    return Reference(cities), {c.city_code: c for c in cities}, zips


def order_zips(cur) -> pd.DataFrame:
    """(order_id, raw city_id, zip_code) for every t_order row; zip from the latest stg row of the order."""
    cur.execute(f'SELECT channel_id, channel_code FROM {SCHEMA}.m_channel')
    channel = {code: cid for cid, code in cur.fetchall()}
    frames = []
    for code, table in STG.items():
        m = STG_MAP[code]
        cur.execute(f"""
            SELECT DISTINCT ON ({m['order_no']}) btrim({m['order_no']}), {m['zip']}
            FROM {SCHEMA}.{table}
            WHERE {m['zip']} ~ '[0-9]{{5}}'
            ORDER BY {m['order_no']}, load_run_id DESC, source_row_no
        """)
        z = pd.DataFrame(cur.fetchall(), columns=['order_no', 'raw'])
        z['zip_code'] = [extract_zip(v, m['zip_style']) for v in z['raw']]
        z['channel_id'] = channel[code]
        frames.append(z.drop(columns='raw'))
        print(f'  {code}: {z["zip_code"].notna().sum():,} orders with a postal code in {table}')
    zips = pd.concat(frames, ignore_index=True).dropna(subset=['zip_code'])
    cur.execute(f'SELECT order_id, channel_id, order_no, city_id FROM {SCHEMA}.t_order')
    orders = pd.DataFrame(cur.fetchall(), columns=['order_id', 'channel_id', 'order_no', 'city_id'])
    return orders.merge(zips, on=['channel_id', 'order_no'], how='left')


def plan(cur):
    ref, by_code, ref_zips = read_reference()
    zip_city = {z['zip_code']: z['city_code'] for z in ref_zips}
    zip_other = {z['zip_code']: {x.split(':')[0] for x in z['other_city_codes'].split()} for z in ref_zips}
    cur.execute(f"""
        SELECT c.city_id, c.city_name, c.province_name, c.city_code
        FROM {SCHEMA}.m_city c ORDER BY c.city_id
    """)
    m_city = pd.DataFrame(cur.fetchall(), columns=['city_id', 'city_name', 'province_name', 'city_code'])
    raw = m_city[m_city['city_code'].isna()].copy()
    print(f'm_city: {len(m_city):,} rows, {len(raw):,} without city_code (raw wording)')
    orders = order_zips(cur)
    orders['zip_city_code'] = orders['zip_code'].map(zip_city)
    print(f't_order: {len(orders):,} rows, {orders["zip_code"].notna().sum():,} with a postal code, '
          f'{orders["zip_city_code"].notna().sum():,} of them in the reference')

    ev = orders[orders['zip_city_code'].notna()].groupby('city_id')['zip_city_code'].agg(lambda s: Counter(s))
    n_orders = orders.groupby('city_id').size()
    canon_ids = dict(zip(m_city.loc[m_city['city_code'].notna(), 'city_code'],
                         m_city.loc[m_city['city_code'].notna(), 'city_id']))
    rows = []
    for r in raw.itertuples():
        m = ref.match(r.city_name, r.province_name)
        name_code = m.city.city_code if m.city else None
        counts = ev.get(r.city_id, Counter())
        n_zip = sum(counts.values())
        top, top_n = counts.most_common(1)[0] if counts else (None, 0)
        share = top_n / n_zip if n_zip else None
        code, method, flags = name_code, m.method, [f for f in (m.flag or '').split(', ') if f]
        if top and n_zip >= ZIP_MIN and share >= ZIP_SHARE and top != name_code:
            if name_code:
                flags.append('name_zip_conflict')       # name kept (user decision 2026-11-13)
            else:
                code, method = top, 'zip_majority'
        if code is None:
            method = 'unresolved'
        target = by_code.get(code)
        rows.append({
            'raw_city_id': r.city_id, 'raw_city_name': r.city_name, 'raw_province_name': r.province_name,
            'orders': int(n_orders.get(r.city_id, 0)),
            'name_match_code': name_code, 'name_match': m.city.city_name if m.city else None,
            'zip_orders': n_zip, 'zip_top_code': top, 'zip_top': by_code[top].city_name if top else None,
            'zip_top_share': round(share, 3) if share is not None else None,
            'city_code': code, 'city_name': target.city_name if target else None,
            'province_name': target.province_name if target else None,
            'match_method': method, 'dq_flag': ', '.join(dict.fromkeys(flags)) or None,
        })
    alias = pd.DataFrame(rows)
    # per order: own postal code first, else the wording's city
    word_code = dict(zip(alias['raw_city_id'], alias['city_code']))
    for cid, code in canon_ids.items():          # already canonical rows (re-run)
        word_code.setdefault(code, cid)
    orders['word_city_code'] = orders['city_id'].map(word_code)
    orders['final_code'] = [
        pick_city(None if pd.isna(zc) else zc, zip_other.get(z), None if pd.isna(wc) else wc)
        for z, zc, wc in zip(orders['zip_code'], orders['zip_city_code'], orders['word_city_code'])]
    return ref, by_code, ref_zips, m_city, alias, orders


def report(out: Path, by_code, ref_zips, alias, orders):
    out.mkdir(parents=True, exist_ok=True)
    o_method = alias.groupby('match_method')['orders'].sum()
    diff = orders[orders['zip_city_code'].notna() & orders['word_city_code'].notna()
                  & (orders['zip_city_code'] != orders['word_city_code'])].copy()
    diff['decided_by'] = (diff['final_code'] == diff['zip_city_code']).map({True: 'zip', False: 'wording (zip in several cities)'})
    final_cities = orders['final_code'].dropna().nunique()
    summary = pd.DataFrame([
        ('m_city raw wordings', len(alias)),
        ('… matched by name (exact / normalised)', int(alias['match_method'].isin(['name_exact', 'name_normalized']).sum())),
        ('… bare name taken as Kota (flagged)', int((alias['match_method'] == 'bare_name_kota').sum())),
        ('… zip majority (zip overrode or filled the name)', int((alias['match_method'] == 'zip_majority').sum())),
        ('… unresolved (kept, dq_flag unresolved)', int((alias['match_method'] == 'unresolved').sum())),
        ('orders total', len(orders)),
        ('orders on unresolved wording', int(o_method.get('unresolved', 0))),
        ('orders with a postal code in the reference', int(orders['zip_city_code'].notna().sum())),
        ('orders whose postal code city differs from the wording city', len(diff)),
        ('… decided by the postal code', int((diff['decided_by'] == 'zip').sum())),
        ('… kept the wording (reference lists the code in several cities, wording names one)',
         int((diff['decided_by'] != 'zip').sum())),
        ('name matches whose zip majority disagrees (name kept, flagged)',
         int(alias['dq_flag'].fillna('').str.contains('name_zip_conflict').sum())),
        ('distinct cities in t_order before', int(orders['city_id'].nunique())),
        ('distinct cities in t_order after', final_cities),
        ('reference kabupaten/kota', len(by_code)),
        ('reference postal codes (one city each)', len(ref_zips)),
        ('… listed in >1 city by the reference (most villages wins)',
         sum(1 for z in ref_zips if z['dq_flag'] == 'multi_city_in_reference')),
    ], columns=['Item', 'Count'])
    canon = (alias.dropna(subset=['city_code']).groupby(['city_code', 'city_name', 'province_name'])
             .agg(wordings=('raw_city_name', 'size'), orders=('orders', 'sum'),
                  examples=('raw_city_name', lambda s: ' | '.join(list(dict.fromkeys(s))[:8])))
             .reset_index().sort_values('orders', ascending=False))
    conflicts = (diff.groupby(['zip_code', 'zip_city_code', 'word_city_code', 'decided_by']).size().rename('orders')
                 .reset_index().sort_values('orders', ascending=False))
    conflicts['zip_city'] = conflicts['zip_city_code'].map(lambda c: by_code[c].city_name)
    conflicts['wording_city'] = conflicts['word_city_code'].map(lambda c: by_code[c].city_name)
    multi = pd.DataFrame([z for z in ref_zips if z['dq_flag']])
    if not multi.empty:
        multi['city_name'] = multi['city_code'].map(lambda c: by_code[c].city_name)
        multi['other_cities'] = multi['other_city_codes'].map(
            lambda s: ', '.join(f"{by_code[x.split(':')[0]].city_name} ({x.split(':')[1]})" for x in s.split()))
    path = out / 'city_cleanup_review.xlsx'
    with pd.ExcelWriter(path) as xw:
        summary.to_excel(xw, sheet_name='Summary', index=False)
        canon.to_excel(xw, sheet_name='Canonical Cities', index=False)
        alias.sort_values(['match_method', 'orders'], ascending=[True, False]).to_excel(
            xw, sheet_name='Wording Map', index=False)
        alias[alias['match_method'] == 'unresolved'].sort_values('orders', ascending=False).to_excel(
            xw, sheet_name='Unresolved', index=False)
        alias[alias['dq_flag'].notna()].sort_values('orders', ascending=False).to_excel(
            xw, sheet_name='Flagged Matches', index=False)
        conflicts.to_excel(xw, sheet_name='Zip vs Wording', index=False)
        multi.to_excel(xw, sheet_name='Reference Multi-City Zips', index=False)
    print(summary.to_string(index=False))
    print(f'review workbook: {path}')


def backup(cur, out: Path):
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    for name, sql in [('m_city', f'SELECT * FROM {SCHEMA}.m_city'),
                      ('t_order_city', f'SELECT order_id, city_id, zip_code FROM {SCHEMA}.t_order')]:
        path = out / f'backup_{name}_{stamp}.csv'
        with open(path, 'w', encoding='utf-8') as f:
            cur.copy_expert(f'COPY ({sql}) TO STDOUT WITH CSV HEADER', f)
        print(f'backup: {path}')


def apply(conn, by_code, ref_zips, m_city, alias, orders, commit=True):
    with conn.cursor() as cur:
        for code in STG:
            cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (f'orders:{code}',))
        before = _totals(cur)
        # 1. canonical rows (reuse a raw row already spelled exactly like the reference)
        c = list(by_code.values())
        cur.execute(f"""
            INSERT INTO {SCHEMA}.m_city (city_name, province_name, city_code, city_type, province_code)
            SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::text[])
            ON CONFLICT (city_name, province_name) DO UPDATE SET
                city_code = EXCLUDED.city_code, city_type = EXCLUDED.city_type,
                province_code = EXCLUDED.province_code, dq_flag = NULL
            RETURNING city_code, city_id
        """, ([x.city_name for x in c], [x.province_name for x in c], [x.city_code for x in c],
              [x.city_type for x in c], [x.province_code for x in c]))
        cid = dict(cur.fetchall())
        # 2. postal codes
        cur.execute(f"""
            INSERT INTO {SCHEMA}.m_zipcode (zip_code, city_id, dq_flag, other_city_ids, source)
            SELECT z.zip_code, z.city_id, z.dq_flag, NULLIF(string_to_array(z.others, ' ')::int[], '{{}}'), z.source
            FROM unnest(%s::text[], %s::int[], %s::text[], %s::text[], %s::text[])
                 AS z(zip_code, city_id, dq_flag, others, source)
            ON CONFLICT (zip_code) DO UPDATE SET city_id = EXCLUDED.city_id, dq_flag = EXCLUDED.dq_flag,
                other_city_ids = EXCLUDED.other_city_ids, source = EXCLUDED.source
        """, ([z['zip_code'] for z in ref_zips], [cid[z['city_code']] for z in ref_zips],
              [z['dq_flag'] or None for z in ref_zips],
              [' '.join(str(cid[x.split(':')[0]]) for x in z['other_city_codes'].split()) for z in ref_zips],
              [SOURCE] * len(ref_zips)))
        # 3. wording -> city (unresolved wording points at its own raw row)
        reused = set(cid.values())
        target = {r.raw_city_id: (cid[r.city_code] if r.city_code else r.raw_city_id) for r in alias.itertuples()}
        cur.execute(f"""
            INSERT INTO {SCHEMA}.m_city_alias (raw_city_name, raw_province_name, city_id, match_method, dq_flag)
            SELECT * FROM unnest(%s::text[], %s::text[], %s::int[], %s::text[], %s::text[])
            ON CONFLICT (raw_city_name, raw_province_name) DO UPDATE SET city_id = EXCLUDED.city_id,
                match_method = EXCLUDED.match_method, dq_flag = EXCLUDED.dq_flag
        """, (list(alias['raw_city_name']), list(alias['raw_province_name']),
              [target[i] for i in alias['raw_city_id']], list(alias['match_method']),
              [None if pd.isna(f) else f for f in alias['dq_flag']]))
        unresolved = [int(i) for i in alias.loc[alias['match_method'] == 'unresolved', 'raw_city_id'] if i not in reused]
        cur.execute(f"UPDATE {SCHEMA}.m_city SET dq_flag = 'unresolved' WHERE city_id = ANY(%s)", (unresolved,))
        # 4. orders: own postal code, else wording
        o = orders.copy()
        o['new_city_id'] = o['final_code'].map(cid)
        o['new_city_id'] = o['new_city_id'].fillna(o['city_id'].map(target)).fillna(o['city_id'])
        o['new_city_id'] = o['new_city_id'].astype('Int64')
        cur.execute('CREATE TEMP TABLE tmp_city (order_id bigint PRIMARY KEY, city_id int, zip_code text) ON COMMIT DROP')
        buf = io.StringIO()
        o[['order_id', 'new_city_id', 'zip_code']].to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)
        cur.copy_expert("COPY tmp_city FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
        cur.execute(f"""
            UPDATE {SCHEMA}.t_order t SET city_id = m.city_id, zip_code = m.zip_code
            FROM tmp_city m
            WHERE t.order_id = m.order_id
              AND (t.city_id IS DISTINCT FROM m.city_id OR t.zip_code IS DISTINCT FROM m.zip_code)
        """)
        print(f't_order rows updated: {cur.rowcount:,}')
        cur.execute(f"""
            UPDATE {SCHEMA}.dm_sales_daily d SET city_id = k.new_id
            FROM unnest(%s::int[], %s::int[]) AS k(old_id, new_id)
            WHERE d.city_id = k.old_id AND k.old_id <> k.new_id
        """, (list(target), list(target.values())))
        # 5. drop the raw rows nothing points at any more
        cur.execute(f"""
            DELETE FROM {SCHEMA}.m_city c
            WHERE c.city_code IS NULL AND c.dq_flag IS NULL
              AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.t_order t WHERE t.city_id = c.city_id)
              AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.dm_sales_daily d WHERE d.city_id = c.city_id)
              AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.m_city_alias a WHERE a.city_id = c.city_id)
              AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.m_zipcode z WHERE z.city_id = c.city_id)
        """)
        print(f'raw m_city rows deleted: {cur.rowcount:,}')
        after = _totals(cur)
        if before != after:
            raise RuntimeError(f't_order totals changed: {before} -> {after}')
        cur.execute(f"SELECT count(*) FROM {SCHEMA}.m_city WHERE city_code IS NULL AND dq_flag IS NULL")
        left = cur.fetchone()[0]
        if left:
            raise RuntimeError(f'{left} raw m_city rows are still referenced')
        cur.execute(f"""SELECT count(*), count(DISTINCT city_id), count(zip_code) FROM {SCHEMA}.t_order""")
        print('t_order rows / distinct cities / with zip:', cur.fetchone())
        cur.execute(f"SELECT count(*), count(city_code), count(*) FILTER (WHERE dq_flag = 'unresolved') FROM {SCHEMA}.m_city")
        print('m_city rows / canonical / unresolved:', cur.fetchone())
    if commit:
        conn.commit()
        print('committed')
    else:
        conn.rollback()
        print('rehearsal: rolled back, nothing written')


def _totals(cur):
    cur.execute(f'SELECT channel_id, count(*), sum(gmv_2) FROM {SCHEMA}.t_order GROUP BY 1 ORDER BY 1')
    return cur.fetchall()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dsn', default=DEFAULT_DSN)
    ap.add_argument('--out', default=str(ROOT / 'analysis' / 'city_cleanup'))
    ap.add_argument('--apply', action='store_true', help='write to the database (default: dry run)')
    ap.add_argument('--rehearse', action='store_true', help='run all writes and checks, then roll back')
    a = ap.parse_args()
    out = Path(a.out)
    conn = psycopg2.connect(a.dsn)
    try:
        with conn.cursor() as cur:
            ref, by_code, ref_zips, m_city, alias, orders = plan(cur)
        report(out, by_code, ref_zips, alias, orders)
        if a.apply:
            with conn.cursor() as cur:
                backup(cur, out)
            apply(conn, by_code, ref_zips, m_city, alias, orders)
        elif a.rehearse:
            apply(conn, by_code, ref_zips, m_city, alias, orders, commit=False)
        else:
            print('dry run: nothing written to the database (use --apply after reviewing the workbook)')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
