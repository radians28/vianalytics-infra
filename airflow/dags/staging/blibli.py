"""Blibli seller-center "bulk order download" export -> royal_canin.stg_blibli.

The export's `Data` sheet holds one row per order item (44 columns); the
`Legend` sheet only describes the columns and is ignored. Row 1 is the header,
and any row whose Nomor pesanan is not numeric (blank, notes, totals) is dropped.

Blibli's "Seller Analytics" dashboard download looks similar by name but is a
sales summary with no order lines; it is refused instead of loaded.
"""
import json
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

TABLE = 'stg_blibli'
CHANNEL_CODE = 'BLIBLI'
# Sheet of the bulk order download; a re-saved workbook may have others
SHEET_NAME = 'Data'
# Columns that identify an order export (stg column -> header in the file)
REQUIRED_COLUMNS = {
    'nomor_pesanan': 'Nomor pesanan',
    'nomor_item_pesanan': 'Nomor Item pesanan',
    'tanggal_pesanan': 'Tanggal pesanan',
    'sku_blibli': 'SKU Blibli',
}
# Numeric IDs a spreadsheet may have turned into numbers before a CSV save
ID_COLUMNS = ('nomor_pesanan', 'nomor_item_pesanan', 'nomor_paket')
ORDER_ID = re.compile(r'\d+')
# An ID a spreadsheet turned into a number before the CSV was saved: 1.21882E+10
SCIENTIFIC_ID = re.compile(r'\d+(\.\d+)?E\+?\d+', re.I)
# First cell of every sheet of the Seller Analytics dashboard download
DASHBOARD_TITLE = 'blibli seller analytics'
# Blibli's bulk download stops at this many rows; a file this size is likely cut off
EXPORT_ROW_CAP = 1000


def load(upload: dict, conn_id: str = 'vianalytics_db') -> dict:
    """Stream the uploaded file into stg_blibli in batches of BATCH_SIZE.

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
            if counts['message']:
                _record_warning(status_cur, upload['upload_id'], counts['message'])

    log.info('upload %s: %s', upload['upload_id'], counts)
    return {
        **upload,
        'load_run_id': run_id,
        'stg_rows': {TABLE: counts['inserted']},
        'rejected_rows': counts['rejected'],
    }


def _insert_rows(cur, status_cur, upload: dict, rows, run_id: int) -> dict:
    raw_header = next(rows, [])
    header = [snake(h) if h is not None else '' for h in raw_header]
    missing_required = [label for col, label in REQUIRED_COLUMNS.items() if col not in header]
    if missing_required:
        first_cell = str(raw_header[0] if raw_header else '').strip().lower()
        if first_cell.startswith(DASHBOARD_TITLE):
            raise ValueError(
                'Not a Blibli order export: this is the Blibli Seller Analytics sales summary '
                '(no order lines). Upload the bulk order download (sheet "Data") instead'
            )
        raise ValueError(
            f'Not a Blibli order export: missing column(s) {", ".join(missing_required)}'
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

    order_idx = header.index('nomor_pesanan')
    id_positions = [(c, header.index(c)) for c in ID_COLUMNS if c in header]
    counts = {'read': 0, 'inserted': 0, 'rejected': 0}
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
        for column, idx in id_positions:
            value = values[idx] if idx < len(values) else None
            if value and SCIENTIFIC_ID.fullmatch(value):
                raise PrecisionLossError(
                    f'Row {row_no}: {column} {value} was saved in scientific notation and has '
                    'lost digits; format the ID columns as Text before saving the CSV'
                )
        order_id = values[order_idx] if order_idx < len(values) else None
        if not order_id or not ORDER_ID.fullmatch(order_id):
            counts['rejected'] += 1                      # blank / note / total rows
            continue

        buffer.append([values[i] if i is not None and i < len(values) else None
                       for i in positions] + [upload['file_name'], row_no, run_id])
        if len(buffer) >= BATCH_SIZE:
            flush()
            report_progress(status_cur, upload['upload_id'], counts['read'])
    flush()

    if not counts['inserted']:
        raise ValueError('No order rows found in the file')
    counts['message'] = None
    if counts['inserted'] == EXPORT_ROW_CAP:
        # Still loaded: the rows are valid, there may just be more of them
        counts['message'] = (
            f'File has exactly {EXPORT_ROW_CAP} order rows, the Blibli bulk download limit; '
            'it is probably cut off. Download the period in smaller date ranges and upload those too'
        )
        log.warning('upload %s: %s', upload['upload_id'], counts['message'])
    return counts


def _record_warning(status_cur, upload_id: str, warning: str):
    """Keep a non-fatal warning on the upload (meta_data.warning) for the UI."""
    status_cur.execute("""
        UPDATE upload_progress
        SET meta_data = (coalesce(meta_data::jsonb, '{}'::jsonb) || %(patch)s::jsonb)::json,
            updated_at = now()
        WHERE upload_id = %(upload_id)s
    """, {'upload_id': upload_id, 'patch': json.dumps({'warning': warning})})
