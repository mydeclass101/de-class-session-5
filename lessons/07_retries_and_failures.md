# บทที่ 7 · เมื่องานพัง ⭐

**เป้าหมาย:** ให้ Airflow ลองใหม่เองกับปัญหาชั่วคราว พังทันทีกับปัญหาที่ลองใหม่ไม่หาย และแจ้งเตือนเมื่อพัง

ตัวอย่างเต็ม: `dags/lessons/lesson_07_retries_and_failures.py` (`lesson_07_failures` พังโดยตั้งใจ)

🔁 session 4 บท 5.4: "ลองใหม่เฉพาะปัญหาชั่วคราว (เน็ตหลุด) รหัสผิดลองกี่ทีก็ไม่หาย" วันนี้ไม่ต้องเขียน loop ลองใหม่เองแล้ว

## 7.1 ⌨️ retries

สร้าง `dags/my/my_07_retries.py`

```python
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=1),
    },
    tags=["my"],
)
def my_07_retries():

    @task
    def call_payment_api(**context):
        attempt = context["ti"].try_number
        if attempt < 3:
            raise ConnectionError(f"ครั้งที่ {attempt}: ต่อ API ไม่ได้")
        print(f"ครั้งที่ {attempt}: สำเร็จ")

    call_payment_api()


my_07_retries()
```

| ค่า | ความหมาย |
|---|---|
| `retries=3` | พังแล้วลองใหม่ได้อีก 3 ครั้ง (รวมสูงสุด 4 ครั้ง) |
| `retry_delay` | รอก่อนลองใหม่ |
| `retry_exponential_backoff=True` | รอนานขึ้นเรื่อย ๆ (เว็บกำลังล่ม อย่ากระหน่ำ) |
| `max_retry_delay` | แต่รอไม่เกินนี้ |
| `context["ti"].try_number` | ครั้งที่เท่าไหร่ (เริ่ม 1) |

กด Trigger แล้วดู Grid: ช่องเป็น `up_for_retry` 2 รอบแล้วเขียว · คลิกช่อง → Logs มีแท็บแยกตามครั้งที่ลอง

## 7.2 ⌨️ พังทันที / หมดเวลา / แจ้งเตือน

```python
from airflow.sdk import TriggerRule
from airflow.sdk.exceptions import AirflowFailException


def tell_the_team(context):
    ti = context["ti"]
    print(f"แจ้งเตือน: {ti.dag_id}.{ti.task_id} พัง")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=3), "on_failure_callback": tell_the_team},
    tags=["my"],
)
def my_07_failures():

    @task
    def check_file_columns():
        raise AirflowFailException("ไฟล์ไม่มีคอลัมน์ order_id")

    @task(execution_timeout=timedelta(seconds=5), retries=0)
    def slow_query():
        import time
        time.sleep(30)

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def alert_on_call():
        print("เปิด ticket ให้คนเวรตรวจสอบ")

    @task
    def publish_report():
        print("ส่งรายงาน")

    checks = [check_file_columns(), slow_query()]
    checks >> alert_on_call()
    checks >> publish_report()


my_07_failures()
```

| เขียน | ผล |
|---|---|
| `raise AirflowFailException(...)` | พังทันที **ไม่ลองใหม่** แม้ตั้ง retries (ข้อมูลผิด, ไฟล์ผิดรูปแบบ) |
| `raise` error อื่น ๆ | พัง แล้วลองใหม่ตาม `retries` (ปัญหาชั่วคราว) |
| `execution_timeout=...` | รันนานเกิน = ถูกหยุดและนับว่าพัง (กันงานค้างทั้งคืน) |
| `dagrun_timeout=...` (ใน `@dag`) | ทั้งรอบห้ามนานเกิน |
| `on_failure_callback=f` | Airflow เรียก `f(context)` เมื่อ task พัง (ของจริงส่ง Slack/อีเมล) มี `on_success_callback`, `on_retry_callback` ด้วย |
| `trigger_rule=ONE_FAILED` | task แจ้งเตือนที่รันเฉพาะเมื่อมีตัวพัง |

ผล: `publish_report` = `upstream_failed` (ไม่ได้รันเพราะตัวก่อนหน้าพัง) · ทั้ง DAG run = failed

## 7.3 แก้แล้วรันใหม่

| หน้าเว็บ | ใช้เมื่อ |
|---|---|
| คลิก task → **Clear** | แก้โค้ด/ข้อมูลแล้ว อยากรัน task นั้น (และตัวที่ตามมา) ใหม่ |
| คลิก task → **Mark as → success** | ยอมรับผลไปก่อน ให้ตัวที่ตามมารันต่อ |
| คลิก run → **Clear** | รันทั้งรอบใหม่ |

💡 Clear ไม่ได้สร้างรอบใหม่ แต่รันรอบเดิม (ช่วงเวลาเดิม) ซ้ำ งานจึงต้อง **รันซ้ำแล้วผลไม่เพี้ยน** (session 6 จะลงรายละเอียด)

## 🧪 ลองเอง

1. แก้ `call_payment_api` ให้สำเร็จครั้งที่ 5 โดยไม่เพิ่ม `retries` ดูว่าเกิดอะไรขึ้น แล้วแก้ให้ผ่าน
2. ใน `my_07_failures` แก้ `check_file_columns` ให้ไม่พัง แล้ว Clear เฉพาะ task นั้น `publish_report` จะรันไหม (ดู `slow_query`)
3. ใน `my_07_retries` เพิ่ม `"on_retry_callback"` ใน `default_args` ที่ print `"กำลังจะลองใหม่"` แล้วหาข้อความนี้ใน Logs

## ✅ สรุป

| ปัญหา | ใช้ |
|---|---|
| ชั่วคราว (เน็ต, เว็บล่ม) | `retries`, `retry_delay`, `retry_exponential_backoff` |
| ถาวร (ข้อมูลผิด) | `raise AirflowFailException(...)` |
| ค้าง | `execution_timeout`, `dagrun_timeout` |
| ต้องรู้เมื่อพัง | `on_failure_callback`, task `trigger_rule=ONE_FAILED` |
| รันใหม่ | Clear |
