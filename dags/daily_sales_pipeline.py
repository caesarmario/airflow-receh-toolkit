import csv
import pendulum
from pathlib import Path
from airflow.sdk import dag, task

INPUT_FILE = Path("/opt/airflow/data/orders.csv")
OUTPUT_FILE = Path("/opt/airflow/output/daily_sales.csv")

@dag(
    dag_id="daily_sales_pipeline",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    tags=["workshop"]
)
def daily_sales_pipeline():

    @task
    def extract():
        """TASK 1: Extract data from input CSV file."""
        with INPUT_FILE.open(newline="", encoding="utf-8") as file:
            orders = list(csv.DictReader(file))
        print(f"Read {len(orders)} orders.")
        return orders

    @task
    def transform(orders):
        """TASK 2: Transform the extracted data."""
        daily = {}

        for order in orders:
            if order["status"] != "paid":
                continue

            date = order["order_date"]
            amount = int(order["amount"])

            if date not in daily:
                daily[date] = {"date": date, "total_orders": 0, "revenue": 0}

            daily[date]["total_orders"] += 1
            daily[date]["revenue"] += amount

        return [daily[date] for date in sorted(daily)]

    @task
    def validate(sales):
        """TASK 3: Validate transformed data."""
        if not sales:
            raise ValueError("No paid orders found!")

        for row in sales:
            if row["total_orders"] <= 0 or row["revenue"] < 0:
                raise ValueError(f"Invalid sales data: {row}")

        print("Sales data validation passed!")
        return sales

    @task
    def publish(sales):
        """TASK 4: Publish validated data."""

        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=["date", "total_orders", "revenue"])
            writer.writeheader()
            writer.writerows(sales)

        print(f"Saved report to {OUTPUT_FILE}.")

    orders = extract()
    sales = transform(orders)
    checked = validate(sales)
    publish(checked)


daily_sales_pipeline()