import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id="my_first_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False
)
def my_first_pipeline():
    @task
    def say_hello():
        print("Halo, Airflow!")
    
    say_hello()

my_first_pipeline()