"""Shopee "Order.all" export -> royal_canin.stg_shopee.

Sheet `orders`, row 1 is the header and every row after it one order line
(order-level fields repeat on each line). There is no description row; rows
whose No. Pesanan is not a Shopee order number (blank, totals, notes) are
rejected. Large months come split into "part_N_of_M" files, each uploaded and
loaded on its own.
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
    open_source,
    PrecisionLossError,
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

TABLE = 'stg_shopee'
CHANNEL_CODE = 'SHOPEE'
# Order SN: order date YYMMDD + alphanumeric tail, e.g. 220101KF7XQ33K
ORDER_ID = re.compile(r'\d{6}[0-9A-Z]+')
# Some order SNs are all digits (23011370177505); a spreadsheet turns those
# into numbers and a CSV save then writes 2.30114E+13
SCIENTIFIC_ID = re.compile(r'\d+(\.\d+)?E\+?\d+', re.I)
# Sheet of the order export; other sheets in a re-saved workbook are ignored
SHEET_NAME = 'orders'
# Columns that identify the file as a Shopee order export
REQUIRED_COLUMNS = {'no_pesanan': 'No. Pesanan',
                    'waktu_pesanan_dibuat': 'Waktu Pesanan Dibuat',
                    'nomor_referensi_sku': 'Nomor Referensi SKU'}


def load(upload: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Stream the uploaded file into stg_shopee in batches of BATCH_SIZE.

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
                source = open_source(upload['file_path'], sheet_names=(SHEET_NAME,))
                log.info('upload %s: reading %s file', upload['upload_id'], source.format)
                if source.total_rows is not None:
                    # every row after the header, so processed_rows ends equal to it
                    set_total_rows(status_cur, upload['upload_id'], max(source.total_rows - 1, 0))

                run_id = start_load_run(cur, upload, [TABLE])
                data_conn.commit()      # keep the run row even if the data rolls back

                counts = _insert_rows(cur, status_cur, upload, source.rows, run_id)
                log_step(cur, run_id, 1, 'load_stg', CHANNEL_CODE, TABLE,
                         rows_read=counts['read'],
                         rows_inserted=counts['inserted'],
                         rows_rejected=counts['rejected'])
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
        'stg_rows': {TABLE: counts['inserted']},
        'rejected_rows': counts['rejected'],
    }


def _insert_rows(cur, status_cur, upload: dict, rows, run_id: int) -> dict:
    header = [snake(h) if h is not None else '' for h in next(rows, [])]
    missing_required = [label for col, label in REQUIRED_COLUMNS.items() if col not in header]
    if missing_required:
        raise ValueError('Not a Shopee order export: missing column(s) '
                         + ', '.join(missing_required))

    columns = stg_columns(cur, TABLE)
    missing = [c for c in columns if c not in header]
    if missing:
        log.warning('%s: columns not in file, left NULL: %s', TABLE, missing)
    extra = sorted(set(header) - set(columns) - {''})
    if extra:
        log.warning('file columns not in staging table, ignored: %s', extra)
    sql = insert_sql(TABLE, columns)
    positions = [header.index(c) if c in header else None for c in columns]

    order_idx = header.index('no_pesanan')
    counts = {'read': 0, 'rejected': 0, 'inserted': 0}
    buffer = []

    def flush():
        if buffer:
            cur.executemany(sql, buffer)
            counts['inserted'] += len(buffer)
            buffer.clear()

    for row_no, row in enumerate(rows, start=2):        # file row number (header = 1)
        counts['read'] += 1
        try:
            values = [cell_text(v) for v in row]
        except PrecisionLossError as err:
            raise PrecisionLossError(f'Row {row_no}: {err}') from None
        order_id = values[order_idx] if order_idx < len(values) else None
        if order_id and SCIENTIFIC_ID.fullmatch(order_id):
            raise PrecisionLossError(
                f'Row {row_no}: No. Pesanan {order_id} was saved in scientific notation and '
                'has lost digits; format the ID columns as Text before saving the CSV'
            )
        if not order_id or not ORDER_ID.fullmatch(order_id):
            counts['rejected'] += 1                      # blank / total / note rows
            continue

        buffer.append([values[i] if i is not None and i < len(values) else None
                       for i in positions] + [upload['file_name'], row_no, run_id])
        if len(buffer) >= BATCH_SIZE:
            flush()
            report_progress(status_cur, upload['upload_id'], counts['read'])

    flush()
    if not counts['inserted']:
        raise ValueError('No order rows found in the file')
    return counts
