"""Lazada order export ("Order_<date>.xlsx", sheet `sheet1`) -> royal_canin.stg_lazada.

One row per order item; row 1 is the camelCase header (orderItemId,
orderNumber, sellerSku, ...), which snake() maps onto the 77 DDL columns. Rows
without a numeric orderNumber (blank or footer rows) are dropped.
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

TABLE = 'stg_lazada'
CHANNEL_CODE = 'LAZADA'
# Sheet of the order export; other sheets in a re-saved workbook are ignored
SHEET_NAME = 'sheet1'
# Columns that identify a Lazada order export
REQUIRED_COLUMNS = ('order_number', 'order_item_id', 'seller_sku')
# Long numeric IDs; exported as text, but a re-saved file may have turned them into numbers
ID_COLUMNS = ('order_number', 'order_item_id', 'lazada_id')
ORDER_NUMBER = re.compile(r'\d+')
# An ID a spreadsheet turned into a number before the CSV was saved: 2.70325E+15
SCIENTIFIC_ID = re.compile(r'\d+(\.\d+)?E\+?\d+', re.I)
# Excel keeps 15 significant digits, so a numeric ID of 16+ digits (Lazada IDs
# are 15-16) has lost its last digit even though it is below 2**53, where
# cell_text's own check starts
EXCEL_DIGITS = 10 ** 15


def load(upload: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Stream the uploaded file into stg_lazada in batches of BATCH_SIZE.

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
                         rows_inserted=counts['inserted'][TABLE],
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
        'stg_rows': counts['inserted'],
        'rejected_rows': counts['rejected'],
    }


def _insert_rows(cur, status_cur, upload: dict, rows, run_id: int) -> dict:
    header = [snake(h) if h is not None else '' for h in next(rows, [])]
    absent = [c for c in REQUIRED_COLUMNS if c not in header]
    if absent:
        raise ValueError(
            'Not a Lazada order export: missing column(s) '
            + ', '.join(absent) + ' (expected orderNumber, orderItemId and sellerSku)'
        )

    columns = stg_columns(cur, TABLE)
    missing = [c for c in columns if c not in header]
    if missing:
        log.warning('%s: columns not in file, left NULL: %s', TABLE, missing)
    extra = sorted(set(header) - set(columns) - {''})
    if extra:
        log.warning('file columns not in %s, ignored: %s', TABLE, extra)
    sql = insert_sql(TABLE, columns)
    positions = [header.index(c) if c in header else None for c in columns]
    id_positions = {header.index(c): c for c in ID_COLUMNS if c in header}

    order_idx = header.index('order_number')
    counts = {'read': 0, 'rejected': 0, 'inserted': {TABLE: 0}}
    buffer = []

    def flush():
        if buffer:
            cur.executemany(sql, buffer)
            counts['inserted'][TABLE] += len(buffer)
            buffer.clear()

    for row_no, row in enumerate(rows, start=2):        # file row number (header = 1)
        counts['read'] += 1
        _check_ids(row, row_no, id_positions)
        try:
            values = [cell_text(v) for v in row]
        except PrecisionLossError as err:
            raise PrecisionLossError(f'Row {row_no}: {err}') from None
        order_number = values[order_idx] if order_idx < len(values) else None
        if not order_number or not ORDER_NUMBER.fullmatch(order_number):
            counts['rejected'] += 1                      # blank / footer rows
            continue

        buffer.append([values[i] if i is not None and i < len(values) else None
                       for i in positions] + [upload['file_name'], row_no, run_id])
        if len(buffer) >= BATCH_SIZE:
            flush()
            report_progress(status_cur, upload['upload_id'], counts['read'])

    flush()
    if not counts['inserted'][TABLE]:
        raise ValueError('No order rows found in the file')
    return counts


def _check_ids(row: list, row_no: int, id_positions: dict[int, str]):
    """Fail on an ID column whose digits a spreadsheet has already dropped."""
    for i, column in id_positions.items():
        value = row[i] if i < len(row) else None
        if isinstance(value, (int, float)) and not isinstance(value, bool) and abs(value) >= EXCEL_DIGITS:
            raise PrecisionLossError(
                f'Row {row_no}: {column} {value:.0f} is stored as a number in Excel, which keeps '
                'only 15 digits; format the ID columns as Text and export again'
            )
        if isinstance(value, str) and SCIENTIFIC_ID.fullmatch(value.strip()):
            raise PrecisionLossError(
                f'Row {row_no}: {column} {value.strip()} was saved in scientific notation and has '
                'lost digits; format the ID columns as Text before saving the CSV'
            )
