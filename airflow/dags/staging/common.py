"""Shared pieces for loading an uploaded marketplace export into royal_canin.stg_*.

Staging columns are text, as-is from the file (see ddl_royal_canin.sql). Every
load is one royal_canin.log_etl_run row; stg rows carry its run_id in
load_run_id, so a retried upload can drop what an earlier attempt inserted.
"""
import csv
import os
import re
from collections.abc import Iterator
from datetime import date, datetime

SCHEMA = 'royal_canin'
BATCH_SIZE = 1000

# stg columns filled by the loader, not by the file
META_COLUMNS = {'stg_id', 'source_file', 'source_row_no', 'load_run_id', 'loaded_at'}


def snake(header) -> str:
    """'Nomor Referensi SKU' -> 'nomor_referensi_sku' (matches the DDL's stg column names)."""
    h = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', str(header))      # camelCase -> camel_case
    return re.sub(r'[^a-z0-9]+', '_', h.lower()).strip('_')


def cell_text(value) -> str | None:
    """Cell -> stripped text as written in the file; empty cells become NULL."""
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)          # 12.0 -> '12', never '12.0'
    elif isinstance(value, datetime):
        value = value.strftime('%d/%m/%Y %H:%M:%S')
    elif isinstance(value, date):
        value = value.strftime('%d/%m/%Y')
    text = str(value).strip()
    return text or None


def iter_file_rows(file_path: str) -> tuple[int | None, Iterator[list]]:
    """Stream the first sheet of an xlsx/xls (or a csv) as row lists, header first.

    Returns (row count including the header if known, rows). Uses calamine:
    TikTok/Tokopedia exports have a sheet xml that openpyxl cannot read.
    """
    extension = os.path.splitext(file_path)[1].lower()
    if extension == '.csv':
        def csv_rows():
            with open(file_path, newline='', encoding='utf-8-sig') as f:
                yield from csv.reader(f)
        return None, csv_rows()

    from python_calamine import CalamineWorkbook

    sheet = CalamineWorkbook.from_path(file_path).get_sheet_by_index(0)
    return sheet.height, sheet.iter_rows()


def stg_columns(cur, table: str) -> list[str]:
    """Data columns of royal_canin.<table>, in table order."""
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %(schema)s AND table_name = %(table)s
        ORDER BY ordinal_position
    """, {'schema': SCHEMA, 'table': table})
    columns = [row[0] for row in cur.fetchall()]
    if not columns:
        raise RuntimeError(f'Staging table {SCHEMA}.{table} does not exist')
    return [c for c in columns if c not in META_COLUMNS]


def insert_sql(table: str, columns: list[str]) -> str:
    names = columns + ['source_file', 'source_row_no', 'load_run_id']
    return 'INSERT INTO {}.{} ({}) VALUES ({})'.format(
        SCHEMA, table, ', '.join(names), ', '.join(['%s'] * len(names))
    )


def start_load_run(cur, upload: dict, tables: list[str]) -> int:
    """Open a log_etl_run for this attempt and drop rows from earlier attempts
    of the same upload (Airflow retries), so the load stays idempotent."""
    triggered_by = f"upload:{upload['upload_id']}"
    cur.execute(f"""
        SELECT run_id FROM {SCHEMA}.log_etl_run WHERE triggered_by = %(triggered_by)s
    """, {'triggered_by': triggered_by})
    previous = [row[0] for row in cur.fetchall()]
    if previous:
        for table in tables:
            cur.execute(
                f'DELETE FROM {SCHEMA}.{table} WHERE load_run_id = ANY(%(runs)s)',
                {'runs': previous},
            )
        cur.execute(f"""
            UPDATE {SCHEMA}.log_etl_run
            SET status = 'FAILED', finished_at = now(),
                error_message = coalesce(error_message, 'superseded by a retry')
            WHERE run_id = ANY(%(runs)s) AND status = 'RUNNING'
        """, {'runs': previous})

    cur.execute(f"""
        INSERT INTO {SCHEMA}.log_etl_run (status, channels, source_files, triggered_by)
        VALUES ('RUNNING', %(channel)s, %(file_path)s, %(triggered_by)s)
        RETURNING run_id
    """, {'channel': upload['channel'], 'file_path': upload['file_path'],
          'triggered_by': triggered_by})
    return cur.fetchone()[0]


def log_step(cur, run_id: int, step_seq: int, step_name: str, channel_code: str,
             table: str, rows_read: int, rows_inserted: int, rows_rejected: int,
             message: str | None = None):
    cur.execute(f"""
        INSERT INTO {SCHEMA}.log_etl_step (run_id, step_seq, step_name, channel_code,
            target_table, status, rows_read, rows_inserted, rows_rejected, message)
        VALUES (%(run_id)s, %(step_seq)s, %(step_name)s, %(channel_code)s,
            %(table)s, 'SUCCESS', %(rows_read)s, %(rows_inserted)s, %(rows_rejected)s, %(message)s)
    """, {'run_id': run_id, 'step_seq': step_seq, 'step_name': step_name,
          'channel_code': channel_code, 'table': f'{SCHEMA}.{table}', 'rows_read': rows_read,
          'rows_inserted': rows_inserted, 'rows_rejected': rows_rejected, 'message': message})


def finish_load_run(cur, run_id: int, error: str | None = None):
    cur.execute(f"""
        UPDATE {SCHEMA}.log_etl_run
        SET status = %(status)s, finished_at = now(), error_message = %(error)s
        WHERE run_id = %(run_id)s
    """, {'status': 'FAILED' if error else 'SUCCESS', 'error': error, 'run_id': run_id})


def mark_processing(cur, upload_id: str, total_rows: int | None):
    cur.execute("""
        UPDATE upload_progress
        SET status = 'Processing', total_rows = %(total_rows)s, processed_rows = 0,
            error_message = NULL, processed_at = now(), last_heartbeat = now(), updated_at = now()
        WHERE upload_id = %(upload_id)s
    """, {'upload_id': upload_id, 'total_rows': total_rows})


def set_total_rows(cur, upload_id: str, total_rows: int):
    cur.execute("""
        UPDATE upload_progress SET total_rows = %(total_rows)s, updated_at = now()
        WHERE upload_id = %(upload_id)s
    """, {'upload_id': upload_id, 'total_rows': total_rows})


def report_progress(cur, upload_id: str, processed_rows: int):
    cur.execute("""
        UPDATE upload_progress
        SET processed_rows = %(processed_rows)s, last_heartbeat = now(), updated_at = now()
        WHERE upload_id = %(upload_id)s
    """, {'upload_id': upload_id, 'processed_rows': processed_rows})


def mark_failed(cur, upload_id: str, error: str):
    cur.execute("""
        UPDATE upload_progress
        SET status = 'Failed', error_message = %(error)s, last_heartbeat = now(), updated_at = now()
        WHERE upload_id = %(upload_id)s
    """, {'upload_id': upload_id, 'error': error})
