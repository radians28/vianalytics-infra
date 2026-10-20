"""TikTok Shop + Tokopedia "OrderSKUList" export -> royal_canin.stg_tiktok / stg_tokopedia.

Both marketplaces come in one export; rows are routed by the `Purchase
Channel` column, not by the file name. Row 1 is the header and row 2 a column
description row, which is dropped with any other row whose Order ID is not
numeric.
"""
import logging
import re
from contextlib import closing

from airflow.providers.postgres.hooks.postgres import PostgresHook

from staging.common import (
    BATCH_SIZE,
    cell_text,
    finish_load_run,
    insert_sql,
    iter_file_rows,
    log_step,
    mark_failed,
    mark_processing,
    report_progress,
    set_total_rows,
    snake,
    start_load_run,
    stg_columns,
)

log = logging.getLogger(__name__)

# Purchase Channel value (lower case) -> staging table
TABLES = {'tiktok': 'stg_tiktok', 'tokopedia': 'stg_tokopedia'}
ORDER_ID = re.compile(r'\d+')


def load(upload: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Stream the uploaded file into the staging tables in batches of BATCH_SIZE.

    The data goes in one transaction (all or nothing); upload_progress and
    log_etl_run are written on a separate autocommit connection so the UI sees
    progress and failures are recorded even when the data is rolled back.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    with closing(hook.get_conn()) as status_conn, closing(hook.get_conn()) as data_conn:
        status_conn.autocommit = True  # pyright: ignore[reportAttributeAccessIssue]
        with status_conn.cursor() as status_cur, data_conn.cursor() as cur:
            mark_processing(status_cur, upload['upload_id'], total_rows=None)
            run_id = None
            try:
                total_height, rows = iter_file_rows(upload['file_path'])
                if total_height is not None:
                    # every row after the header, so processed_rows ends equal to it
                    set_total_rows(status_cur, upload['upload_id'], max(total_height - 1, 0))

                run_id = start_load_run(cur, upload, list(TABLES.values()))
                data_conn.commit()      # keep the run row even if the data rolls back

                counts = _insert_rows(cur, status_cur, upload, rows, run_id)
                for seq, (key, table) in enumerate(TABLES.items(), start=1):
                    log_step(cur, run_id, seq, 'load_stg', key.upper(), table,
                             rows_read=counts['read'],
                             rows_inserted=counts['inserted'][table],
                             rows_rejected=counts['rejected'],
                             message=counts['message'])
                finish_load_run(cur, run_id)
                data_conn.commit()
            except Exception as err:
                data_conn.rollback()  # pyright: ignore[reportAttributeAccessIssue]
                if run_id is not None:
                    finish_load_run(status_cur, run_id, error=str(err))
                mark_failed(status_cur, upload['upload_id'], str(err))
                raise
            report_progress(status_cur, upload['upload_id'], counts['read'])

    log.info('upload %s: %s', upload['upload_id'], counts)
    return {
        **upload,
        'load_run_id': run_id,
        'stg_rows': counts['inserted'],
        'rejected_rows': counts['rejected'],
    }


def _insert_rows(cur, status_cur, upload: dict, rows, run_id: int) -> dict:
    header = [snake(h) for h in next(rows, [])]
    if 'order_id' not in header or 'purchase_channel' not in header:
        raise ValueError('Not a TikTok/Tokopedia order export: missing Order ID or Purchase Channel column')

    # Both tables share the 65-column layout, but each is read from the DB so a
    # DDL change on one table is not silently lost
    plans = {}
    table_columns = set()
    for table in TABLES.values():
        columns = stg_columns(cur, table)
        table_columns.update(columns)
        missing = [c for c in columns if c not in header]
        if missing:
            log.warning('%s: columns not in file, left NULL: %s', table, missing)
        positions = [header.index(c) if c in header else None for c in columns]
        plans[table] = (insert_sql(table, columns), positions, [])
    extra = sorted(set(header) - table_columns)
    if extra:
        log.warning('file columns not in staging tables, ignored: %s', extra)

    order_idx = header.index('order_id')
    channel_idx = header.index('purchase_channel')
    counts = {'read': 0, 'rejected': 0, 'unknown_channel': 0,
              'inserted': {t: 0 for t in TABLES.values()}}

    def flush(table):
        sql, _, buffer = plans[table]
        if buffer:
            cur.executemany(sql, buffer)
            counts['inserted'][table] += len(buffer)
            buffer.clear()

    for row_no, row in enumerate(rows, start=2):        # excel row number (header = 1)
        counts['read'] += 1
        order_id = cell_text(row[order_idx]) if order_idx < len(row) else None
        if not order_id or not ORDER_ID.fullmatch(order_id):
            counts['rejected'] += 1                      # description / blank rows
            continue
        channel = (cell_text(row[channel_idx]) or '').lower() if channel_idx < len(row) else ''
        table = TABLES.get(channel)
        if table is None:
            counts['rejected'] += 1
            counts['unknown_channel'] += 1
            continue

        _, positions, buffer = plans[table]
        buffer.append([cell_text(row[i]) if i is not None and i < len(row) else None
                       for i in positions] + [upload['file_name'], row_no, run_id])
        if len(buffer) >= BATCH_SIZE:
            flush(table)
            report_progress(status_cur, upload['upload_id'], counts['read'])

    for table in TABLES.values():
        flush(table)

    counts['message'] = None
    if counts['unknown_channel']:
        counts['message'] = f"{counts['unknown_channel']} rows with unknown Purchase Channel skipped"
        log.warning(counts['message'])
    if not any(counts['inserted'].values()):
        raise ValueError('No order rows found in the file')
    return counts
