"""บทที่ 0 · เช็กเครื่อง: Airflow ทำงาน และต่อฐานข้อมูล ecommerce ได้"""
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(
    schedule=None,                                                  # ไม่ตั้งเวลา: กดรันเองเท่านั้น
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-00"],
)
def lesson_00_setup_check():

    @task
    def hello():
        print("Airflow ทำงานแล้ว")

    @task
    def count_orders() -> int:
        hook = PostgresHook(postgres_conn_id="ecommerce_db")
        total = hook.get_first("SELECT count(*) FROM orders")[0]
        print(f"ตาราง orders มี {total:,} แถว")
        return total

    hello() >> count_orders()


lesson_00_setup_check()
