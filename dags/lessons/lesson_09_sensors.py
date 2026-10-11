"""บทที่ 9 · Sensor: รอจนกว่าเงื่อนไขจะเป็นจริง แล้วค่อยไปต่อ

ลองเอง: กด Trigger แล้ว task wait_for_partner_file จะรอ (สีฟ้าอ่อน up_for_reschedule)
ใน VS Code สร้างไฟล์ output/inbox/partner_orders.csv (ใส่อะไรก็ได้) ภายใน 2 นาที task จะไปต่อเอง
ถ้าไม่สร้าง ครบ 2 นาทีจะพัง (timeout)
"""
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import PokeReturnValue, dag, task

INBOX = Path("/opt/airflow/output/inbox")              # = โฟลเดอร์ output/inbox ในเครื่องเรา


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-09"],
)
def lesson_09_sensors():

    # 1) @task.sensor: ฟังก์ชันถูกเรียกซ้ำทุก poke_interval จนกว่าจะคืน is_done=True หรือหมดเวลา timeout
    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def wait_for_partner_file() -> PokeReturnValue:
        INBOX.mkdir(parents=True, exist_ok=True)
        files = sorted(p.name for p in INBOX.glob("*.csv"))
        print("ไฟล์ที่เห็นตอนนี้:", files)
        return PokeReturnValue(is_done=len(files) > 0, xcom_value=files)   # xcom_value ส่งต่อให้ task ถัดไป

    @task
    def read_files(files: list[str]):
        for name in files:
            print("อ่านไฟล์", name)

    # 2) Sensor แบบ deferrable: ระหว่างรอไม่กิน worker เลย (ไปฝากให้ triggerer เฝ้าแทน สถานะ deferred สีม่วง)
    wait_a_bit = TimeDeltaSensor(task_id="wait_10_seconds", delta=timedelta(seconds=10), deferrable=True)

    wait_a_bit >> read_files(wait_for_partner_file())


lesson_09_sensors()
