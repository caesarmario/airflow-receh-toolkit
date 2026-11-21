import csv
import sqlite3
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

REPORT_FILE = Path("/opt/airflow/output/daily_sales.csv")
DB_FILE = Path("/opt/airflow/output/sales.db")

@dag(
    dag_id="load_daily_sales_to_sqlite",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=["workshop"]
)
def load_daily_sales_to_sqlite():

    @task
    def read_report():
        """TASK 1: Read output report CSV file"""
        if not REPORT_FILE.exists():
            raise FileNotFoundError(f"Report file not found: {REPORT_FILE}. Run daily_sales_pipeline first.")

        with REPORT_FILE.open(newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))

        if not rows:
            raise ValueError("Daily sales report is empty!")

        print(f"Read {len(rows)} rows from report file CSV")
        return rows

    @task
    def load_database(rows):
        DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(DB_FILE)

        try:
            with connection:
                connection.execute("""
                    CREATE TABLE IF NOT EXISTS daily_sales (
                        date TEXT PRIMARY KEY,
                        total_orders INTEGER NOT NULL,
                        revenue INTEGER NOT NULL
                    )
                """)
                 # Replace the previous report, to avoid duplicate rows.
                connection.execute("DELETE FROM daily_sales")

                for row in rows:
                    connection.execute(
                        "INSERT INTO daily_sales (date, total_orders, revenue) VALUES (?, ?, ?)",
                        (row["date"], int(row["total_orders"]), int(row["revenue"]))
                    )
        finally:
            connection.close()
        print(f"Saved {len(rows)} rows to SQLite database at {DB_FILE}")
        return len(rows)

    @task
    def check_database(expected_rows):
        connection = sqlite3.connect(DB_FILE)
        try:
            rows = connection.execute("SELECT * FROM daily_sales ORDER by date").fetchall()
        finally:
            connection.close()
        
        if len(rows) != expected_rows:
            raise ValueError("Database row count does not match expected")

        for row in rows:
            print(row)

        print("Database check passed!")

    rows = read_report()
    saved = load_database(rows)
    check_database(saved)


load_daily_sales_to_sqlite()