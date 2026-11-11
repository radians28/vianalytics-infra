"""rematch_pending_skus: match every listing still waiting for review against the current master.

A PENDING row of royal_canin.log_sku_match was scored against the master as it was when its upload
ran. Reviews and later uploads add SKUs, listings and seller SKUs since then, so each run re-decides
every PENDING row (all channels) with the same rules as process_sku (sku/process.py):

  master now knows                      -> action                          -> row
  seller SKU, not the listing           add_platform (4a)                   MAPPED auto_mapped
  the listing, not its seller SKU       add_alias + history version (4b)    MAPPED auto_mapped
  both, same SKU (or listing, no code)  -                                   MAPPED auto_mapped
  both, different SKUs                  log_dq listing_sku_conflict (once)  stays PENDING
  neither, close name match             candidates / suggestion refreshed   stays PENDING
  neither, no close match               new m_sku + listing                 CREATED_NEW rematch_created

The lines of rows resolved here were skipped by their upload's order load, so their resolutions are
in orders.status.CONTINUE_RESOLUTIONS. A Paused upload left with nothing pending is returned in
`resume`; the DAG resumes it with handle_continue_order_data_{channel}.

Locking: the SKU master advisory lock first (as process_sku and the svc review do; ids are max + 1),
waited for at most LOCK_TIMEOUT - the task retries while an upload or a review is writing. Rows are
then claimed FOR UPDATE SKIP LOCKED: a row another process holds is left for the next run. The
order loads (first and continue) take neither lock, so they run alongside.
"""
import json
import logging
from contextlib import closing
from dataclasses import dataclass, field

from airflow.providers.postgres.hooks.postgres import PostgresHook

from orders.status import CONTINUE_RESOLUTIONS
from sku.matcher import Candidate, is_close
from sku.process import (LOCK_KEY, Listing, Master, add_alias, add_platform, change_seller_sku,
                         create_sku, refresh_waiting_uploads, touch_sku)
from staging.common import SCHEMA

log = logging.getLogger(__name__)

LOCK_TIMEOUT = '30s'

# decide() actions
MAP_NEW_LISTING = 'map_new_listing'     # seller SKU known, listing new
MAP_NEW_CODE = 'map_new_code'           # listing known, seller SKU new
MAP = 'map'                             # both known and agree
CONFLICT = 'conflict'                   # both known, different SKUs
REFRESH = 'refresh'                     # name still close: new candidate list
CREATE = 'create'                       # no close name any more: new SKU


@dataclass
class Decision:
    action: str
    sku_id: int | None = None
    candidates: list[Candidate] = field(default_factory=list)


def decide(book, channel_id: int, x: Listing) -> Decision:
    """What the current master says about one waiting listing (book: sku.process.Master or a stand-in
    with .platform, .sku_of_code() and .matcher())."""
    platform = book.platform.get((channel_id, x.merchant_sku))
    sku_id = book.sku_of_code(x.sku_code)
    if platform is None and sku_id is not None:
        return Decision(MAP_NEW_LISTING, sku_id)
    if platform is not None:
        if sku_id is None:
            return Decision(MAP_NEW_CODE if x.sku_code else MAP, platform['sku_id'])
        if sku_id == platform['sku_id']:
            return Decision(MAP, sku_id)
        return Decision(CONFLICT, platform['sku_id'])
    candidates = book.matcher().top(x.product_name)
    return Decision(REFRESH if is_close(candidates[0] if candidates else None) else CREATE, None, candidates)


@dataclass
class Counts:
    pending: int = 0
    skipped_locked: int = 0
    mapped: int = 0
    created: int = 0
    refreshed: int = 0
    conflicts: int = 0

    def summary(self) -> dict:
        return dict(self.__dict__)


def run(conf: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Re-decide every PENDING listing; returns counts and the uploads to resume."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    counts = Counts()
    with closing(hook.get_conn()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
                cur.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK_KEY,))
                cur.execute("SET LOCAL lock_timeout = '0'")
                rows = _claim(cur, counts)
                book = Master(cur)
                per_upload = _process(cur, book, rows, counts)
                _record(cur, conf, per_upload)
                conn.commit()
            except Exception:
                conn.rollback()  # pyright: ignore[reportAttributeAccessIssue]
                raise
        resume = _resume_targets(conn)
    log.info('rematch %s: %s; resume %s', conf.get('source'), counts.summary(), [r['upload_id'] for r in resume])
    return {**conf, 'rematch': counts.summary(), 'resume': resume}


def _claim(cur, counts: Counts) -> list[dict]:
    cur.execute(f"SELECT count(*) FROM {SCHEMA}.log_sku_match WHERE status = 'PENDING'")
    counts.pending = cur.fetchone()[0]
    cur.execute(f"""
        SELECT match_id, run_id, channel_id, merchant_sku, sku_code, seller_sku, product_name, variation,
               first_order_at, last_order_at, order_line_count
        FROM {SCHEMA}.log_sku_match WHERE status = 'PENDING'
        ORDER BY match_id FOR UPDATE SKIP LOCKED
    """)
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    counts.skipped_locked = counts.pending - len(rows)
    return rows


def _listing(row: dict) -> Listing:
    return Listing(merchant_sku=row['merchant_sku'], sku_code=row['sku_code'] or None,
                   seller_sku=row['seller_sku'], product_name=row['product_name'], variation=row['variation'],
                   first_order_at=row['first_order_at'], last_order_at=row['last_order_at'],
                   lines=row['order_line_count'] or 0)


def _process(cur, book: Master, rows: list[dict], counts: Counts) -> dict[int, str]:
    """Apply decide() to every claimed row in match_id order; returns {match_id: outcome} of changed rows."""
    changed = {}
    for row in rows:
        x = _listing(row)
        channel_id = row['channel_id']
        d = decide(book, channel_id, x)
        if d.action == MAP_NEW_LISTING:
            if x.sku_code not in book.alias:                      # known only from m_sku_history
                add_alias(cur, book, x, d.sku_id)
            add_platform(cur, book, channel_id, x, d.sku_id)
            touch_sku(cur, book, d.sku_id, x)
        elif d.action == MAP_NEW_CODE:
            platform = book.platform[(channel_id, x.merchant_sku)]
            add_alias(cur, book, x, d.sku_id)
            change_seller_sku(cur, book, platform, x, d.sku_id)
            touch_sku(cur, book, d.sku_id, x)
        elif d.action == MAP:
            touch_sku(cur, book, d.sku_id, x)
        elif d.action == CONFLICT:
            _log_conflict(cur, row, book.sku_of_code(x.sku_code))
            counts.conflicts += 1
            continue
        elif d.action == REFRESH:
            _save_candidates(cur, row['match_id'], d.candidates)
            counts.refreshed += 1
            changed[row['match_id']] = 'refreshed'
            continue
        else:                                                     # CREATE
            d.sku_id = create_sku(cur, book, x)
            add_platform(cur, book, channel_id, x, d.sku_id)
            _resolve(cur, row['match_id'], 'CREATED_NEW', d.sku_id, 'rematch_created', d.candidates)
            counts.created += 1
            changed[row['match_id']] = 'created'
            continue
        _resolve(cur, row['match_id'], 'MAPPED', d.sku_id, 'auto_mapped')
        counts.mapped += 1
        changed[row['match_id']] = 'mapped'

    resolved = [m for m, outcome in changed.items() if outcome != 'refreshed']
    if resolved:
        refresh_waiting_uploads(cur, resolved)
    return changed


def _best(candidates: list[Candidate]) -> dict:
    best = candidates[0] if candidates else None
    return {'suggested_sku_id': best.sku_id if best else None, 'suggested_name': best.name if best else None,
            'score': best.score if best else None,
            'candidates': json.dumps([c.as_json() for c in candidates])}


def _save_candidates(cur, match_id: int, candidates: list[Candidate]):
    cur.execute(f"""
        UPDATE {SCHEMA}.log_sku_match
        SET candidates = %(candidates)s::jsonb, suggested_sku_id = %(suggested_sku_id)s,
            suggested_name = %(suggested_name)s, similarity_score = %(score)s
        WHERE match_id = %(id)s AND status = 'PENDING'
    """, {**_best(candidates), 'id': match_id})


def _resolve(cur, match_id: int, status: str, sku_id: int, resolution: str,
             candidates: list[Candidate] | None = None):
    params = {'status': status, 'sku': sku_id, 'resolution': resolution, 'id': match_id}
    refresh = ''
    if candidates is not None:
        params.update(_best(candidates))
        refresh = """, candidates = %(candidates)s::jsonb, suggested_sku_id = %(suggested_sku_id)s,
            suggested_name = %(suggested_name)s, similarity_score = %(score)s"""
    cur.execute(f"""
        UPDATE {SCHEMA}.log_sku_match
        SET status = %(status)s, resolved_sku_id = %(sku)s, resolved_by = 'auto', resolved_at = now(),
            resolution = %(resolution)s{refresh}
        WHERE match_id = %(id)s AND status = 'PENDING'
    """, params)


def _log_conflict(cur, row: dict, code_sku_id: int | None):
    """One log_dq_issue per waiting listing whose listing and seller SKU belong to different SKUs."""
    key = f"{row['merchant_sku']}|{row['sku_code'] or ''}"
    cur.execute(f"""
        SELECT 1 FROM {SCHEMA}.log_dq_issue
        WHERE dq_flag = 'listing_sku_conflict' AND record_key = %s AND source_table = %s
    """, (key, f'{SCHEMA}.log_sku_match'))
    if cur.fetchone():
        return
    cur.execute(f"""
        INSERT INTO {SCHEMA}.log_dq_issue (run_id, source_table, dq_flag, record_key, sku_id, detail)
        VALUES (%s, %s, 'listing_sku_conflict', %s, %s, %s)
    """, (row['run_id'], f'{SCHEMA}.log_sku_match', key, code_sku_id,
          f"waiting listing {row['merchant_sku']} belongs to another SKU than its seller SKU "
          f"{row['seller_sku']} (sku_id {code_sku_id}); needs a review decision (match {row['match_id']})"))


def _record(cur, conf: dict, changed: dict[int, str]):
    """meta_data.sku_rematch on every upload holding a changed listing: what this run did for it."""
    if not changed:
        return
    cur.execute(f"""
        SELECT upload_id, match_id FROM {SCHEMA}.log_sku_match_upload WHERE match_id = ANY(%s)
    """, (list(changed),))
    per_upload: dict[str, dict] = {}
    for upload_id, match_id in cur.fetchall():
        c = per_upload.setdefault(upload_id, {'mapped': 0, 'created': 0, 'refreshed': 0})
        c[changed[match_id]] += 1
    for upload_id, c in per_upload.items():
        cur.execute("""
            UPDATE upload_progress
            SET meta_data = (coalesce(meta_data::jsonb, '{}'::jsonb)
                             || jsonb_build_object('sku_rematch', %(meta)s::jsonb))::json,
                updated_at = now()
            WHERE upload_id = %(id)s
        """, {'id': upload_id, 'meta': json.dumps({**c, 'at': _now_iso(cur), 'source': conf.get('source'),
                                                    'by': conf.get('by') or 'auto'})})


def _now_iso(cur) -> str:
    cur.execute("SELECT to_char(now() AT TIME ZONE 'Asia/Jakarta', 'YYYY-MM-DD\"T\"HH24:MI:SS+07:00')")
    return cur.fetchone()[0]


def _resume_targets(conn) -> list[dict]:
    """Paused uploads with nothing pending and resolved listings whose lines the continue load takes."""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT u.upload_id, u.channel,
                   array_agg(m.match_id ORDER BY m.match_id)
                       FILTER (WHERE m.status <> 'PENDING' AND m.resolution = ANY(%s)) AS match_ids
            FROM upload_progress u
            JOIN {SCHEMA}.log_sku_match_upload mu ON mu.upload_id = u.upload_id
            JOIN {SCHEMA}.log_sku_match m ON m.match_id = mu.match_id
            WHERE u.status = 'Paused'
            GROUP BY u.upload_id, u.channel
            HAVING count(*) FILTER (WHERE m.status = 'PENDING') = 0
            ORDER BY min(u.queued_at)
        """, (list(CONTINUE_RESOLUTIONS),))
        rows = cur.fetchall()
        conn.commit()
    return [{'upload_id': r[0], 'channel': r[1], 'match_ids': list(r[2])} for r in rows if r[2]]
