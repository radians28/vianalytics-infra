"""process_sku_{channel}: register the SKUs of an uploaded order export in the SKU master.

Runs after the staging load, on the stg rows of that load (load_run_id):
1. stream only the SKU columns of the upload's stg rows (named cursor, BATCH_SIZE rows a
   fetch) and fold them into one listing per (seller SKU, platform SKU)
2. a listing key already seen in this upload only widens its order window and line count
3. each listing is checked against the master
   a. seller SKU: m_sku_alias (every seller SKU code, cleaned) and m_sku_history (a code found
      only in the history is added to m_sku_alias, the lookup the order load resolves through)
   b. platform SKU: m_platform_sku (channel + merchant_sku)
4. a. 3a known, 3b not: the SKU is already listed elsewhere -> new m_platform_sku row
   b. 3b known, 3a not: the listing was sold under another seller SKU -> new m_sku_alias row
      and a new m_sku_history version; the listing's version timeline is rebuilt in order of
      first order date, so the latest seller SKU becomes current_seller_sku and, when it is
      the newest code of its SKU, m_sku.master_sku
   both known with a seller SKU new to the listing: history version as in 4b (logged in
   log_dq_issue when that seller SKU belongs to another master SKU)
5. neither known: the product name is compared with the master's SKU and listing names
   (sku.matcher). A close match is written to log_sku_match as PENDING with the suggested
   SKU, for review in the UI (SKU sanitize modal: map to the suggestion or create a new SKU),
   and its order lines wait for that decision (the DAG goes on; handle_order_dag_{channel} skips
   them and ends the upload as 'Paused'). Without a close match the listing becomes a new
   m_sku (+ m_sku_alias, m_platform_sku), recorded in log_sku_match as CREATED_NEW by 'auto'.

All master writes of one upload are one transaction, serialised across concurrent runs
by an advisory lock; ids are taken as max + 1 under that lock.
"""
import json
import logging
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from sku.matcher import NameMatcher, is_close
from sku.rules import JAKARTA, clean_sku, clean_text, parse_ts, shopee_listing_key, sku_type
from staging.common import BATCH_SIZE, SCHEMA, log_step

log = logging.getLogger(__name__)

# pg_advisory_xact_lock key for SKU master writes ('SKU' in ASCII)
LOCK_KEY = 0x534B55
EPOCH = datetime(1900, 1, 1, tzinfo=JAKARTA)


@dataclass(frozen=True)
class Source:
    channel_code: str
    table: str
    seller_sku: str
    platform_sku: str | None    # None: Shopee, key built from name + variation
    product_name: str
    variation: str | None
    created: str
    ts_format: str


_TIKTOK_TOKPED = dict(seller_sku='seller_sku', platform_sku='sku_id', product_name='product_name',
                      variation='variation', created='created_time', ts_format='%d/%m/%Y %H:%M:%S')
SOURCES = {
    'tiktok_tokped': [Source('TIKTOK', 'stg_tiktok', **_TIKTOK_TOKPED),
                      Source('TOKOPEDIA', 'stg_tokopedia', **_TIKTOK_TOKPED)],
    'shopee': [Source('SHOPEE', 'stg_shopee', 'nomor_referensi_sku', None, 'nama_produk', 'nama_variasi',
                      'waktu_pesanan_dibuat', '%Y-%m-%d %H:%M')],
    'lazada': [Source('LAZADA', 'stg_lazada', 'seller_sku', 'lazada_sku', 'item_name', 'variation',
                      'create_time', '%d %b %Y %H:%M')],
    'blibli': [Source('BLIBLI', 'stg_blibli', 'sku_seller', 'sku_blibli', 'nama_produk', None,
                      'tanggal_pesanan', '%d/%m/%Y %H:%M')],
}


@dataclass
class Listing:
    merchant_sku: str
    sku_code: str | None        # cleaned seller SKU
    seller_sku: str | None      # as in the latest order line
    product_name: str | None
    variation: str | None
    first_order_at: datetime | None = None
    last_order_at: datetime | None = None
    lines: int = 0

    def add(self, seller_sku, name, variation, at):
        self.lines += 1
        if at is not None and (self.first_order_at is None or at < self.first_order_at):
            self.first_order_at = at
        if at is None or self.last_order_at is None or at >= self.last_order_at:
            self.last_order_at = at or self.last_order_at
            self.seller_sku, self.product_name = seller_sku, name or self.product_name
            self.variation = variation or self.variation


@dataclass
class Counts:
    lines: int = 0
    listings: int = 0
    rejected: int = 0
    unchanged: int = 0
    platform_added: int = 0
    alias_added: int = 0
    history_added: int = 0
    sku_created: int = 0
    pending_review: int = 0
    dq_issues: int = 0
    pending: list = field(default_factory=list)

    def summary(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k != 'pending'}


def run(upload: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Register the SKUs of one uploaded file (all its channels); returns the upload with counts."""
    sources = SOURCES[upload['channel']]
    run_id = upload['load_run_id']
    hook = PostgresHook(postgres_conn_id=conn_id)
    # upload_progress turns 'Failed' only after the last retry (staging.common.fail_upload callback)
    with closing(hook.get_conn()) as data_conn:
        with data_conn.cursor() as cur:
            try:
                cur.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK_KEY,))
                book = Master(cur)
                results = {}
                for source in sources:
                    counts = Counts()
                    listings = read_listings(data_conn, source, run_id, counts)
                    register(cur, book, source, listings, upload, counts)
                    log_step(cur, run_id, book.next_step_seq(run_id), 'process_sku', source.channel_code,
                             'm_platform_sku', rows_read=counts.listings,
                             rows_inserted=counts.platform_added + counts.alias_added + counts.sku_created,
                             rows_rejected=counts.rejected, message=json.dumps(counts.summary()))
                    log.info('%s: %s', source.channel_code, counts.summary())
                    results[source.channel_code] = counts
                pending = sum(c.pending_review for c in results.values())
                set_review_meta(cur, upload['upload_id'], run_id, pending)
                data_conn.commit()
            except Exception:
                data_conn.rollback()  # pyright: ignore[reportAttributeAccessIssue]
                raise
    return {
        **upload,
        'sku': {code: c.summary() for code, c in results.items()},
        'sku_review_pending': pending,
    }


# ---------------------------------------------------------------- 1-2. read listings

def read_listings(conn, source: Source, run_id: int, counts: Counts) -> list[Listing]:
    """Stream the upload's stg rows (SKU columns only) into one Listing per key, oldest first."""
    cols = [source.seller_sku, source.platform_sku or 'NULL', source.product_name,
            source.variation or 'NULL', source.created]
    listings: dict[tuple, Listing] = {}
    with conn.cursor(name=f'sku_{source.table}') as stream:
        stream.execute(f"""
            SELECT {', '.join(cols)} FROM {SCHEMA}.{source.table}
            WHERE load_run_id = %(run_id)s ORDER BY stg_id
        """, {'run_id': run_id})
        while rows := stream.fetchmany(BATCH_SIZE):
            for seller_raw, platform_raw, name_raw, variation_raw, created in rows:
                counts.lines += 1
                seller, name, variation = clean_text(seller_raw), clean_text(name_raw), clean_text(variation_raw)
                merchant_sku = (shopee_listing_key(name, variation) if source.platform_sku is None
                                else clean_text(platform_raw))
                if merchant_sku is None:
                    counts.rejected += 1            # no listing id: cannot be a platform SKU
                    continue
                code = clean_sku(seller)
                key = (code, merchant_sku)
                listing = listings.get(key)
                if listing is None:
                    listing = listings[key] = Listing(merchant_sku, code, seller, name, variation)
                listing.add(seller, name, variation, parse_ts(created, source.ts_format))
    counts.listings = len(listings)
    return sorted(listings.values(), key=lambda x: (x.first_order_at or EPOCH, x.merchant_sku, x.sku_code or ''))


# ---------------------------------------------------------------- master lookups

class Master:
    """The SKU master tables as lookups, kept in step with the rows this run inserts."""

    def __init__(self, cur):
        self.cur = cur
        cur.execute(f'SELECT channel_code, channel_id FROM {SCHEMA}.m_channel')
        self.channel_id = dict(cur.fetchall())
        cur.execute(f'SELECT sku_id, sku_master_code, master_sku, product_name, first_order_at FROM {SCHEMA}.m_sku')
        self.sku = {r[0]: {'code': r[1], 'master_sku': r[2], 'name': r[3], 'first_order_at': r[4]}
                    for r in cur.fetchall()}
        cur.execute(f'SELECT sku_code, sku_id FROM {SCHEMA}.m_sku_alias')
        self.alias = dict(cur.fetchall())
        # seller SKUs seen in listing history but missing from the alias table still identify the SKU
        cur.execute(f"""
            SELECT seller_sku, sku_id FROM (
                SELECT old_seller_sku AS seller_sku, sku_id, version FROM {SCHEMA}.m_sku_history
                UNION ALL
                SELECT new_seller_sku, sku_id, version FROM {SCHEMA}.m_sku_history) h
            WHERE seller_sku IS NOT NULL ORDER BY version
        """)
        self.history_code = {}
        for seller_sku, sku_id in cur.fetchall():
            code = clean_sku(seller_sku)
            if code:
                self.history_code[code] = sku_id
        cur.execute(f"""
            SELECT channel_id, merchant_sku, platform_sku_id, sku_id, current_seller_sku, first_order_at
            FROM {SCHEMA}.m_platform_sku
        """)
        self.platform = {(r[0], r[1]): {'id': r[2], 'sku_id': r[3], 'seller_sku': r[4], 'first_order_at': r[5]}
                         for r in cur.fetchall()}
        self.next_id = {}
        for table, col in (('m_sku', 'sku_id'), ('m_platform_sku', 'platform_sku_id'),
                           ('m_sku_history', 'history_id')):
            cur.execute(f'SELECT coalesce(max({col}), 0) FROM {SCHEMA}.{table}')
            self.next_id[table] = cur.fetchone()[0]
        self._matcher = None

    def new_id(self, table: str) -> int:
        self.next_id[table] += 1
        return self.next_id[table]

    def sku_of_code(self, code: str | None) -> int | None:
        if not code:
            return None
        return self.alias.get(code) or self.history_code.get(code)

    def next_step_seq(self, run_id: int) -> int:
        self.cur.execute(f'SELECT coalesce(max(step_seq), 0) + 1 FROM {SCHEMA}.log_etl_step WHERE run_id = %s',
                         (run_id,))
        return self.cur.fetchone()[0]

    def matcher(self) -> NameMatcher:
        """Names of every SKU and listing; rebuilt after this run adds SKUs."""
        if self._matcher is None:
            self.cur.execute(f"""
                SELECT s.sku_id, s.sku_master_code, s.product_name FROM {SCHEMA}.m_sku s
                UNION ALL
                SELECT s.sku_id, s.sku_master_code, p.product_name
                FROM {SCHEMA}.m_platform_sku p JOIN {SCHEMA}.m_sku s USING (sku_id)
                WHERE p.product_name IS NOT NULL
            """)
            self._matcher = NameMatcher(self.cur.fetchall())
        return self._matcher


# ---------------------------------------------------------------- 3-5. register

def register(cur, book: Master, source: Source, listings: list[Listing], upload: dict, counts: Counts):
    channel_id = book.channel_id[source.channel_code]
    for x in listings:
        platform = book.platform.get((channel_id, x.merchant_sku))
        sku_id = book.sku_of_code(x.sku_code)
        if sku_id is not None and x.sku_code not in book.alias:           # known only from m_sku_history
            add_alias(cur, book, x, sku_id)
            counts.alias_added += 1
        if sku_id is not None and platform is None:                       # 4a
            add_platform(cur, book, channel_id, x, sku_id)
            touch_sku(cur, book, sku_id, x)
            counts.platform_added += 1
            resolve_pending(cur, channel_id, x, sku_id)
        elif platform is not None and sku_id is None and x.sku_code:      # 4b
            add_alias(cur, book, x, platform['sku_id'])
            counts.alias_added += 1
            counts.history_added += change_seller_sku(cur, book, platform, x, platform['sku_id'])
            touch_sku(cur, book, platform['sku_id'], x)
        elif platform is not None:                                        # both known (or no seller SKU)
            added = 0
            if sku_id is not None:
                added = change_seller_sku(cur, book, platform, x, sku_id)
                counts.history_added += added
                if added and sku_id != platform['sku_id']:
                    log_dq(cur, upload, 'm_sku_history', 'listing_sku_conflict', x, sku_id,
                           f"listing {source.channel_code} {x.merchant_sku} (sku_id {platform['sku_id']}) "
                           f"sold as {x.seller_sku}, a seller SKU of sku_id {sku_id}")
                    counts.dq_issues += 1
                touch_sku(cur, book, sku_id, x)
            if not added:
                counts.unchanged += 1
        else:                                                             # 5
            match_by_name(cur, book, channel_id, x, upload, counts)


def touch_sku(cur, book: Master, sku_id: int, x: Listing):
    """Earliest order date of the SKU and of the seller SKU code."""
    if x.first_order_at is None:
        return
    sku = book.sku[sku_id]
    if sku['first_order_at'] is None or x.first_order_at < sku['first_order_at']:
        sku['first_order_at'] = x.first_order_at
        cur.execute(f'UPDATE {SCHEMA}.m_sku SET first_order_at = %s WHERE sku_id = %s', (x.first_order_at, sku_id))
    if x.sku_code:
        cur.execute(f"""
            UPDATE {SCHEMA}.m_sku_alias SET first_seen_at = %(at)s
            WHERE sku_code = %(code)s AND (first_seen_at IS NULL OR first_seen_at > %(at)s)
        """, {'at': x.first_order_at, 'code': x.sku_code})


def add_platform(cur, book: Master, channel_id: int, x: Listing, sku_id: int):
    pid = book.new_id('m_platform_sku')
    cur.execute(f"""
        INSERT INTO {SCHEMA}.m_platform_sku (platform_sku_id, channel_id, merchant_sku, sku_id,
            current_seller_sku, product_name, is_on_master_sku, first_order_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (pid, channel_id, x.merchant_sku, sku_id, x.seller_sku, x.product_name,
          x.sku_code is not None and x.sku_code == book.sku[sku_id]['master_sku'], x.first_order_at))
    book.platform[(channel_id, x.merchant_sku)] = {'id': pid, 'sku_id': sku_id, 'seller_sku': x.seller_sku,
                                                   'first_order_at': x.first_order_at}


def add_alias(cur, book: Master, x: Listing, sku_id: int):
    cur.execute(f"""
        INSERT INTO {SCHEMA}.m_sku_alias (sku_code, sku_id, is_master_code, first_seen_at)
        VALUES (%s, %s, false, %s)
    """, (x.sku_code, sku_id, x.first_order_at))
    book.alias[x.sku_code] = sku_id
    promote_master_code(cur, book, sku_id, x.sku_code)


def promote_master_code(cur, book: Master, sku_id: int, code: str):
    """R4 survivorship: the most recently introduced seller SKU of a SKU is its master_sku."""
    cur.execute(f"""
        SELECT sku_code FROM {SCHEMA}.m_sku_alias WHERE sku_id = %s
        ORDER BY first_seen_at DESC NULLS LAST, sku_code LIMIT 1
    """, (sku_id,))
    newest = cur.fetchone()[0]
    if newest != code or book.sku[sku_id]['master_sku'] == code:
        return
    cur.execute(f'UPDATE {SCHEMA}.m_sku_alias SET is_master_code = (sku_code = %s) WHERE sku_id = %s',
                (code, sku_id))
    cur.execute(f'UPDATE {SCHEMA}.m_sku SET master_sku = %s WHERE sku_id = %s', (code, sku_id))
    cur.execute(f"""
        UPDATE {SCHEMA}.m_platform_sku SET is_on_master_sku = (upper(current_seller_sku) = %(code)s
            OR upper(regexp_replace(current_seller_sku, '^(TKP|TTS|LZD|SHP|BLI)[_-]', '', 'i')) = %(code)s)
        WHERE sku_id = %(sku_id)s
    """, {'code': code, 'sku_id': sku_id})
    book.sku[sku_id]['master_sku'] = code


def change_seller_sku(cur, book: Master, platform: dict, x: Listing, sku_id: int) -> int:
    """Add the listing's seller SKU as a new version (SCD 2) unless the listing already had it.

    The versions are rebuilt in order of their start dates, so an older export loaded late slots
    in before the versions that followed it. Returns the number of versions added (0 or 1).
    """
    pid = platform['id']
    cur.execute(f"""
        SELECT old_seller_sku, new_seller_sku, valid_from, changed_at, sku_id
        FROM {SCHEMA}.m_sku_history WHERE platform_sku_id = %s ORDER BY version
    """, (pid,))
    rows = cur.fetchall()
    if rows:
        first = rows[0]
        points = [(first[0], first[2], book.sku_of_code(clean_sku(first[0])) or platform['sku_id'])]
        points += [(r[1], r[3], r[4]) for r in rows]
    else:
        points = [(platform['seller_sku'], platform['first_order_at'], platform['sku_id'])]
    if x.sku_code is None or x.sku_code in {clean_sku(p[0]) for p in points}:
        return 0

    points.append((x.seller_sku, x.first_order_at, sku_id))
    head = points[0] if points[0][0] is None else None             # listing first sold without seller SKU
    points = sorted((p for p in points if p[0] is not None), key=lambda p: p[1] or EPOCH)
    if head is not None:
        points.insert(0, head)
    cur.execute(f'DELETE FROM {SCHEMA}.m_sku_history WHERE platform_sku_id = %s', (pid,))
    for version, (old, new) in enumerate(zip(points, points[1:]), start=2):
        changed_at = new[1] or old[1] or datetime.now(JAKARTA)
        valid_from = old[1] if old[1] is None or old[1] <= changed_at else changed_at
        cur.execute(f"""
            INSERT INTO {SCHEMA}.m_sku_history (history_id, platform_sku_id, sku_id, version,
                old_seller_sku, new_seller_sku, valid_from, changed_at, dq_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (book.new_id('m_sku_history'), pid, new[2], version, old[0], new[0], valid_from, changed_at,
              None if new[2] == platform['sku_id'] else 'listing_sku_conflict'))
        book.history_code[clean_sku(new[0])] = new[2]

    current = points[-1]
    first_at = min((p[1] for p in points if p[1] is not None), default=None)
    cur.execute(f"""
        UPDATE {SCHEMA}.m_platform_sku
        SET current_seller_sku = %s, is_on_master_sku = %s, first_order_at = %s
        WHERE platform_sku_id = %s
    """, (current[0], clean_sku(current[0]) == book.sku[platform['sku_id']]['master_sku'], first_at, pid))
    platform['seller_sku'], platform['first_order_at'] = current[0], first_at
    return 1


# ---------------------------------------------------------------- 5. name match

def match_by_name(cur, book: Master, channel_id: int, x: Listing, upload: dict, counts: Counts):
    candidates = book.matcher().top(x.product_name)
    best = candidates[0] if candidates else None
    if is_close(best):
        save_match(cur, upload, channel_id, x, candidates, 'PENDING', None, None)
        counts.pending_review += 1
        counts.pending.append(x.merchant_sku)
        return

    sku_id = create_sku(cur, book, x)
    add_platform(cur, book, channel_id, x, sku_id)
    save_match(cur, upload, channel_id, x, candidates, 'CREATED_NEW', sku_id, 'auto')
    counts.sku_created += 1
    counts.platform_added += 1
    if x.sku_code:
        counts.alias_added += 1


def create_sku(cur, book: Master, x: Listing) -> int:
    sku_id = book.new_id('m_sku')
    code = 'SKU-{:05d}'.format(sku_id)
    name = x.product_name or x.seller_sku or x.merchant_sku
    cur.execute(f"""
        INSERT INTO {SCHEMA}.m_sku (sku_id, sku_master_code, master_sku, product_name, sku_type,
            bundle_flag, first_order_at, dq_flag)
        VALUES (%s, %s, %s, %s, %s, false, %s, %s)
    """, (sku_id, code, x.sku_code, name, sku_type(x.sku_code, name) if x.sku_code else 'unknown',
          x.first_order_at, 'name_unmatched' if x.sku_code else 'missing_seller_sku,name_unmatched'))
    book.sku[sku_id] = {'code': code, 'master_sku': x.sku_code, 'name': name, 'first_order_at': x.first_order_at}
    if x.sku_code:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.m_sku_alias (sku_code, sku_id, is_master_code, first_seen_at)
            VALUES (%s, %s, true, %s)
        """, (x.sku_code, sku_id, x.first_order_at))
        book.alias[x.sku_code] = sku_id
    book._matcher = None
    return sku_id


def save_match(cur, upload: dict, channel_id: int, x: Listing, candidates: list, status: str,
               resolved_sku_id: int | None, resolved_by: str | None):
    """One review row per listing key; a PENDING row of an earlier upload is refreshed, not duplicated."""
    best = candidates[0] if candidates else None
    values = {
        'run_id': upload['load_run_id'], 'upload_id': upload['upload_id'], 'channel_id': channel_id,
        'merchant_sku': x.merchant_sku, 'sku_code': x.sku_code or '', 'seller_sku': x.seller_sku,
        'product_name': x.product_name, 'variation': x.variation, 'first_order_at': x.first_order_at,
        'last_order_at': x.last_order_at, 'lines': x.lines,
        'suggested_sku_id': best.sku_id if best else None, 'suggested_name': best.name if best else None,
        'score': best.score if best else None,
        'candidates': json.dumps([c.as_json() for c in candidates]),
        'status': status, 'resolved_sku_id': resolved_sku_id, 'resolved_by': resolved_by,
    }
    cur.execute(f"""
        INSERT INTO {SCHEMA}.log_sku_match AS m (run_id, upload_id, channel_id, merchant_sku, sku_code,
            seller_sku, product_name, variation, first_order_at, last_order_at, order_line_count,
            suggested_sku_id, suggested_name, similarity_score, candidates, status, resolved_sku_id,
            resolved_by, resolved_at)
        VALUES (%(run_id)s, %(upload_id)s, %(channel_id)s, %(merchant_sku)s, %(sku_code)s, %(seller_sku)s,
            %(product_name)s, %(variation)s, %(first_order_at)s, %(last_order_at)s, %(lines)s,
            %(suggested_sku_id)s, %(suggested_name)s, %(score)s, %(candidates)s::jsonb, %(status)s,
            %(resolved_sku_id)s, %(resolved_by)s, CASE WHEN %(resolved_by)s IS NULL THEN NULL ELSE now() END)
        ON CONFLICT (channel_id, merchant_sku, sku_code) WHERE status = 'PENDING' DO UPDATE SET
            order_line_count = CASE WHEN m.upload_id = EXCLUDED.upload_id THEN EXCLUDED.order_line_count
                                    ELSE m.order_line_count + EXCLUDED.order_line_count END,
            run_id = EXCLUDED.run_id, upload_id = EXCLUDED.upload_id,
            seller_sku = EXCLUDED.seller_sku, product_name = EXCLUDED.product_name, variation = EXCLUDED.variation,
            first_order_at = LEAST(m.first_order_at, EXCLUDED.first_order_at),
            last_order_at = GREATEST(m.last_order_at, EXCLUDED.last_order_at),
            suggested_sku_id = EXCLUDED.suggested_sku_id, suggested_name = EXCLUDED.suggested_name,
            similarity_score = EXCLUDED.similarity_score, candidates = EXCLUDED.candidates
    """, values)


def resolve_pending(cur, channel_id: int, x: Listing, sku_id: int):
    """A listing waiting for review whose seller SKU has since become known is mapped to that SKU."""
    cur.execute(f"""
        UPDATE {SCHEMA}.log_sku_match
        SET status = 'MAPPED', resolved_sku_id = %s, resolved_by = 'auto', resolved_at = now()
        WHERE channel_id = %s AND merchant_sku = %s AND sku_code = %s AND status = 'PENDING'
    """, (sku_id, channel_id, x.merchant_sku, x.sku_code or ''))


# ---------------------------------------------------------------- status / dq

def log_dq(cur, upload: dict, table: str, flag: str, x: Listing, sku_id: int | None, detail: str):
    cur.execute(f"""
        INSERT INTO {SCHEMA}.log_dq_issue (run_id, source_table, dq_flag, record_key, sku_id, detail)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (upload['load_run_id'], f'{SCHEMA}.{table}', flag, f'{x.merchant_sku}|{x.sku_code or ""}', sku_id, detail))


def set_review_meta(cur, upload_id: str, run_id: int, pending: int):
    """Record how many listings wait for the SKU review. The status is left as is: the DAG goes on to
    the order load, and handle_order_dag_{channel} sets Completed / Paused at the end (orders.status)."""
    cur.execute("""
        UPDATE upload_progress
        SET meta_data = (coalesce(meta_data::jsonb, '{}'::jsonb)
                         || jsonb_build_object('sku_review_pending', %(pending)s, 'sku_review_run_id', %(run_id)s))::json,
            last_heartbeat = now(), updated_at = now()
        WHERE upload_id = %(upload_id)s
    """, {'upload_id': upload_id, 'run_id': run_id, 'pending': pending})
