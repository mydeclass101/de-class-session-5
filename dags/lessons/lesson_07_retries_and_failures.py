"""บทที่ 7 · เมื่องานพัง: retries, timeout, callback และ task แจ้งเตือน

- lesson_07_retries  : เน็ตหลุด 2 ครั้งแรก แล้วครั้งที่ 3 สำเร็จ → Airflow ลองใหม่ให้เอง (DAG สำเร็จ)
- lesson_07_failures : ตั้งใจให้พัง 2 แบบ ดูว่า Airflow ทำอะไรต่อ (DAG พังโดยตั้งใจ)
"""
import time
from datetime import timedelta

import pendulum
from airflow.sdk import TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowFailException


def tell_the_team(context):
    """callback: Airflow เรียกให้เองเมื่อ task พัง (ของจริงส่ง Slack / อีเมล)"""
    ti = context["ti"]
    print(f"แจ้งเตือน: {ti.dag_id}.{ti.task_id} พัง (ลองไป {ti.try_number} ครั้ง)")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args={
        "retries": 3,                                   # พังแล้วลองใหม่อีกได้สูงสุด 3 ครั้ง (รวมเป็น 4 ครั้ง)
        "retry_delay": timedelta(seconds=5),            # รอก่อนลองใหม่
        "retry_exponential_backoff": True,              # รอนานขึ้นเรื่อย ๆ: 5s, 10s, 20s ...
        "max_retry_delay": timedelta(minutes=1),        # แต่ไม่เกิน 1 นาที
        "on_failure_callback": tell_the_team,
    },
    tags=["session5", "lesson-07"],
)
def lesson_07_retries():

    @task
    def call_payment_api(**context) -> str:
        attempt = context["ti"].try_number              # ครั้งที่เท่าไหร่ (เริ่มที่ 1)
        if attempt < 3:
            raise ConnectionError(f"ครั้งที่ {attempt}: ต่อ API ไม่ได้ (ชั่วคราว)")
        print(f"ครั้งที่ {attempt}: สำเร็จ")
        return "ok"

    call_payment_api()


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=3), "on_failure_callback": tell_the_team},
    dagrun_timeout=timedelta(minutes=10),               # ทั้งรอบห้ามเกิน 10 นาที
    tags=["session5", "lesson-07"],
)
def lesson_07_failures():

    @task
    def check_file_columns():
        # ข้อมูลผิดแบบนี้ลองใหม่กี่ครั้งก็ไม่หาย → AirflowFailException = พังทันที ไม่ retry
        raise AirflowFailException("ไฟล์ไม่มีคอลัมน์ order_id: แจ้งคนส่งไฟล์ให้แก้")

    @task(execution_timeout=timedelta(seconds=5), retries=0)
    def slow_query():
        time.sleep(30)                                  # ช้าเกิน 5 วินาที → ถูกหยุดและนับว่าพัง

    @task(trigger_rule=TriggerRule.ONE_FAILED)          # รันเมื่อมีตัวก่อนหน้าพังอย่างน้อย 1 ตัว
    def alert_on_call():
        print("เปิด ticket ให้คนเวรตรวจสอบ")

    @task
    def publish_report():                               # ค่าเริ่มต้น all_success → ตัวก่อนหน้าพัง ตัวนี้ไม่รัน (upstream_failed)
        print("ส่งรายงาน")

    checks = [check_file_columns(), slow_query()]
    checks >> alert_on_call()
    checks >> publish_report()


lesson_07_retries()
lesson_07_failures()
