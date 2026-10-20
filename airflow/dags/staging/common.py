"""Shared pieces for loading an uploaded marketplace export into royal_canin.stg_*.

Staging columns are text, as-is from the file (see ddl_royal_canin.sql). Every
load is one royal_canin.log_etl_run row; stg rows carry its run_id in
load_run_id, so a retried upload can drop what an earlier attempt inserted.
"""
import codecs
import csv
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime

SCHEMA = 'royal_canin'
BATCH_SIZE = 1000

XLSX_SIGNATURE = b'PK\x03\x04'                          # zip container
XLS_SIGNATURE = b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'     # OLE2 compound file
CSV_DELIMITERS = ',;\t|'
CSV_SAMPLE_BYTES = 64 * 1024

# Largest integer a float (Excel number) holds exactly; 18-digit order IDs don't fit
MAX_EXACT_FLOAT = 2 ** 53

# stg columns filled by the loader, not by the file
META_COLUMNS = {'stg_id', 'source_file', 'source_row_no', 'load_run_id', 'loaded_at'}


def snake(header) -> str:
    """'Nomor Referensi SKU' -> 'nomor_referensi_sku' (matches the DDL's stg column names)."""
    h = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', str(header))      # camelCase -> camel_case
    return re.sub(r'[^a-z0-9]+', '_', h.lower()).strip('_')


class PrecisionLossError(ValueError):
    """A long ID was stored as a number, so its last digits are already gone."""


def cell_text(value) -> str | None:
    """Cell -> stripped text as written in the file; empty cells become NULL.

    CSV cells are already text; Excel cells may be numbers or dates.
    """
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        if abs(value) > MAX_EXACT_FLOAT:
            raise PrecisionLossError(
                f'{value:.0f} is stored as a number in Excel and has lost digits; '
                'format the ID columns as Text and export again'
            )
        value = int(value)          # 12.0 -> '12', never '12.0'
    elif isinstance(value, datetime):
        value = value.strftime('%d/%m/%Y %H:%M:%S')
    elif isinstance(value, date):
        value = value.strftime('%d/%m/%Y')
    text = str(value).strip()
    return text or None


@dataclass
class SourceFile:
    format: str                 # 'xlsx', 'xls' or 'csv'
    total_rows: int | None      # rows including the header
    rows: Iterator[list]        # header first


def open_source(file_path: str, sheet_names: tuple[str, ...] = ()) -> SourceFile:
    """Open an uploaded export, Excel or CSV, as a stream of row lists.

    The format comes from the file's content, not its extension, so a CSV saved
    as .xlsx (or the reverse) still loads. For Excel the first sheet named in
    `sheet_names` is used, else the first sheet.
    """
    with open(file_path, 'rb') as f:
        signature = f.read(8)
    if signature.startswith(XLSX_SIGNATURE):
        return _open_excel(file_path, 'xlsx', sheet_names)
    if signature.startswith(XLS_SIGNATURE):
        return _open_excel(file_path, 'xls', sheet_names)
    return _open_csv(file_path)


def _open_excel(file_path: str, file_format: str, sheet_names: tuple[str, ...]) -> SourceFile:
    # calamine, not openpyxl: TikTok/Tokopedia exports have a sheet xml that
    # openpyxl cannot read
    from python_calamine import CalamineWorkbook

    workbook = CalamineWorkbook.from_path(file_path)
    by_name = {name.lower(): name for name in workbook.sheet_names}
    wanted = next((by_name[n.lower()] for n in sheet_names if n.lower() in by_name), None)
    sheet = workbook.get_sheet_by_name(wanted) if wanted else workbook.get_sheet_by_index(0)
    return SourceFile(file_format, sheet.height, sheet.iter_rows())


def _open_csv(file_path: str) -> SourceFile:
    encoding = _detect_encoding(file_path)
    with open(file_path, newline='', encoding=encoding) as f:
        delimiter = _detect_delimiter(f.read(CSV_SAMPLE_BYTES))
        f.seek(0)
        total_rows = sum(1 for _ in csv.reader(f, delimiter=delimiter))

    def rows():
        with open(file_path, newline='', encoding=encoding) as f:
            yield from csv.reader(f, delimiter=delimiter)

    return SourceFile('csv', total_rows, rows())


def _detect_encoding(file_path: str) -> str:
    """UTF-8 (with or without BOM) when the whole file decodes as it, else
    Windows-1252, which Excel on Windows uses for "CSV" saves."""
    decoder = codecs.getincrementaldecoder('utf-8-sig')()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(1024 * 1024):
                decoder.decode(chunk)
            decoder.decode(b'', final=True)
        return 'utf-8-sig'
    except UnicodeDecodeError:
        return 'cp1252'


def _detect_delimiter(sample: str) -> str:
    """Comma, semicolon (Indonesian-locale Excel), tab or pipe separated."""
    try:
        return csv.Sniffer().sniff(sample, delimiters=CSV_DELIMITERS).delimiter
    except csv.Error:
        # Sniffer gives up on uneven samples; go by the header line
        header = sample.splitlines()[0] if sample else ''
        delimiter = max(CSV_DELIMITERS, key=header.count)
        return delimiter if header.count(delimiter) else ','


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
