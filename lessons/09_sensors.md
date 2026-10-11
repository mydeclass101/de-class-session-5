# บทที่ 9 · Sensor: รอจนกว่าจะพร้อม

**เป้าหมาย:** ให้ task รอเงื่อนไข (ไฟล์มาถึง, ข้อมูลพร้อม, ถึงเวลา) แล้วไปต่อเอง โดยไม่กินเครื่องระหว่างรอ

ตัวอย่างเต็ม: `dags/lessons/lesson_09_sensors.py`

## 9.1 ⌨️ `@task.sensor`

สร้าง `dags/my/my_09_sensor.py`

```python
from pathlib import Path

import pendulum
from airflow.sdk import PokeReturnValue, dag, task

INBOX = Path("/opt/airflow/output/inbox")


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["my"])
def my_09_sensor():

    @task.sensor(poke_interval=10, timeout=120, mode="reschedule")
    def wait_for_partner_file() -> PokeReturnValue:
        INBOX.mkdir(parents=True, exist_ok=True)
        files = sorted(p.name for p in INBOX.glob("*.csv"))
        print("ไฟล์ที่เห็นตอนนี้:", files)
        return PokeReturnValue(is_done=len(files) > 0, xcom_value=files)

    @task
    def read_files(files: list[str]):
        for name in files:
            print("อ่านไฟล์", name)

    read_files(wait_for_partner_file())


my_09_sensor()
```

ลอง: ลบไฟล์ใน `output/inbox/` ให้หมด → กด Trigger → task รออยู่ (เปิด Logs ดูทุก 10 วินาที)
→ ใน VS Code สร้างไฟล์ `output/inbox/orders_0920.csv` → ภายใน 10 วินาที task ไปต่อเอง

| ค่า | ความหมาย |
|---|---|
| `poke_interval=10` | เช็กทุก 10 วินาที |
| `timeout=120` | รอนานสุด 120 วินาที เกินนี้ = พัง |
| `mode="reschedule"` | ระหว่างรอ **ปล่อยที่ว่าง** ให้ task อื่น (สถานะ `up_for_reschedule`) · `"poke"` = นั่งรอจองที่ไว้ตลอด เหมาะกับรอสั้น ๆ |
| `PokeReturnValue(is_done=..., xcom_value=...)` | `is_done=True` = ผ่าน · `xcom_value` ส่งต่อให้ task ถัดไป |

💡 `output/` ในเครื่องเรา = `/opt/airflow/output` ในคอนเทนเนอร์ (mount ไว้ใน `docker-compose.yml` 🔁 session 2)

## 9.2 Sensor สำเร็จรูป

| Sensor | import จาก | รออะไร |
|---|---|---|
| `SqlSensor` | `airflow.providers.common.sql.sensors.sql` | query คืนค่าที่ไม่ใช่ 0 (บทที่ 11) |
| `TimeDeltaSensor` | `airflow.providers.standard.sensors.time_delta` | รอเวลาผ่านไป |
| `DateTimeSensor` | `airflow.providers.standard.sensors.date_time` | รอถึงเวลาที่กำหนด |
| `ExternalTaskSensor` | `airflow.providers.standard.sensors.external_task` | รอ task ใน DAG อื่น |
| `FileSensor` | `airflow.providers.standard.sensors.filesystem` | รอไฟล์ (ต้องตั้ง connection `fs_default`) |

## 9.3 deferrable: รอแบบไม่กินเครื่องเลย

```python
from datetime import timedelta
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

wait_a_bit = TimeDeltaSensor(task_id="wait_10_seconds", delta=timedelta(seconds=10), deferrable=True)
```

`deferrable=True` = task ฝากให้ **triggerer** เฝ้าแทน แล้วหลุดออกไปเลย (สถานะ `deferred`) เหมาะกับการรอนาน ๆ หลายตัวพร้อมกัน
Sensor สำเร็จรูปหลายตัวมี `deferrable=True` ให้เลือก

## 🧪 ลองเอง

1. แก้ sensor ให้รอจนกว่าจะมีไฟล์ **อย่างน้อย 2 ไฟล์**
2. กด Trigger โดยไม่สร้างไฟล์ รอจนครบ `timeout` แล้วอ่าน error ใน Logs
3. เพิ่ม `TimeDeltaSensor` แบบ deferrable ก่อน `wait_for_partner_file` แล้วดูสถานะใน Grid ระหว่างรอ

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| รอเงื่อนไขของเราเอง | `@task.sensor(poke_interval=..., timeout=..., mode="reschedule")` + `PokeReturnValue` |
| รอนาน ๆ แบบไม่กินเครื่อง | Sensor สำเร็จรูป + `deferrable=True` |
| อย่ารอตลอดไป | ตั้ง `timeout` เสมอ |
