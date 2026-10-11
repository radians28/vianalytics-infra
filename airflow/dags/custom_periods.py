"""Generate the current year's custom periods (P01-P13) in the app database.

Runs daily at midnight Asia/Jakarta. Idempotent: periods that already exist
are left untouched, so reruns and retries are safe.

Period rules:
  - Week = Saturday..Friday.
  - P01 starts on the Saturday on or before Jan 1 (clipped to Jan 1).
  - P01-P12 are 4 weeks (28 days) each.
  - P13 starts right after P12 and always ends on Dec 31.
"""
from datetime import date, timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

JAKARTA = pendulum.timezone("Asia/Jakarta")


def generate_periods(year: int) -> list[tuple[int, int, str, date, date]]:
    """Return (period_key, year, period, period_start, period_end) rows for `year`."""
    year_start, year_end = date(year, 1, 1), date(year, 12, 31)
    # Saturday on or before Jan 1. Python weekday(): Mon=0 ... Sat=5 ... Sun=6
    start = year_start - timedelta(days=(year_start.weekday() - 5) % 7)

    rows = []
    for p in range(1, 14):
        end = year_end if p == 13 else start + timedelta(days=27)
        rows.append((
            year * 100 + p,        # period_key, e.g. 202601
            year,
            f"P{p:02d}",           # e.g. "P01"
            max(start, year_start),
            end,
        ))
        start = end + timedelta(days=1)
    return rows


@dag(
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=JAKARTA),
    catchup=False,
    tags=["vianalytics"],
)
def ensure_current_year_periods():
    @task
    def ensure_periods(dag_run=None):
        # run_after is set for scheduled and manual runs alike; the run that
        # fires at 00:00 on Jan 1 covers the new year
        year = pendulum.instance(dag_run.run_after).in_timezone(JAKARTA).year
        hook = PostgresHook(postgres_conn_id="vianalytics_db")
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO custom_period (period_key, year, period, period_start, period_end)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (period_key) DO NOTHING
                """,
                generate_periods(year),
            )
        print(f"Ensured custom periods for {year}")

    ensure_periods()


ensure_current_year_periods()
