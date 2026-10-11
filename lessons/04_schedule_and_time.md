# บทที่ 4 · ตั้งเวลา และช่วงเวลาของแต่ละรอบ ⭐

**เป้าหมาย:** ตั้งให้ DAG รันเองทุกวัน แต่ละรอบรู้ว่าต้องดึงข้อมูล **ช่วงไหน** และรันย้อนหลังได้

ตัวอย่างเต็ม: `dags/lessons/lesson_04_schedule_and_time.py`

## 4.1 ช่วงเวลาของแต่ละรอบ (data interval)

🔁 session 4 บท 4.3: ข้อมูล 1 วัน = `[start, end)` รวมต้น ไม่รวมปลาย

งานรายวันที่ดี **แต่ละรอบรับผิดชอบข้อมูล 1 ช่วงที่ตายตัว** ไม่ใช่ "ข้อมูลถึงตอนที่บังเอิญรัน"

```
รอบของวันที่ 20 ก.ย.   data_interval_start = 20 ก.ย. 00:00   data_interval_end = 21 ก.ย. 00:00
                       รันจริงหลังช่วงจบ = 21 ก.ย. 00:00 เป็นต้นไป (ต้องรอให้ข้อมูลวันที่ 20 ครบก่อน)
```

💡 รอบของ "วันที่ 20" **รันตอนต้นวันที่ 21** เพราะต้องรอให้วันที่ 20 จบก่อน

## 4.2 ⌨️ DAG รายวันแบบมีช่วง

สร้าง `dags/my/my_04_daily.py`

```python
import pendulum
from airflow.sdk import CronDataIntervalTimetable, dag, task

BKK = "Asia/Bangkok"


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=BKK),
    start_date=pendulum.datetime(2026, 9, 20, tz=BKK),
    end_date=pendulum.datetime(2026, 9, 24, tz=BKK),
    catchup=True,
    max_active_runs=1,
    tags=["my"],
)
def my_04_daily():

    @task
    def show_context(**context):
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        print("logical_date (UTC) :", context["logical_date"])
        print("ds                 :", context["ds"])
        print("interval (UTC)     :", start, "->", end)
        print("interval (ไทย)     :", start.in_timezone(BKK), "->", end.in_timezone(BKK))

    show_context()


my_04_daily()
```

| ค่า | ความหมาย |
|---|---|
| `CronDataIntervalTimetable("0 0 * * *", timezone=BKK)` | cron = นาที ชั่วโมง วัน เดือน วันในสัปดาห์ → ทุกวัน 00:00 เวลาไทย **รอบละ 1 วันเต็ม** |
| `start_date` | ช่วงแรก = วันที่ 20 ก.ย. |
| `end_date` | ช่วงสุดท้าย = วันที่ 24 ก.ย. (ไม่ใส่ = ไม่มีวันจบ) |
| `catchup=True` | ช่วงที่ผ่านไปแล้วแต่ยังไม่เคยรัน **ให้รันย้อนหลังให้ครบ** (ค่าเริ่มต้นของ Airflow 3 = `False` = รันแค่รอบล่าสุด) |
| `max_active_runs=1` | ย้อนหลังทีละรอบ เรียงวัน |
| `**context` | 🔁 `**kwargs` (session 4 บท 2.4) Airflow ส่ง dict ข้อมูลของรอบนี้มาให้ |

ลองรัน: หน้าเว็บ → `my_04_daily` → **เปิดสวิตช์ (unpause)** → รอสักครู่จะเกิด 5 run (20–24 ก.ย.) เองโดยไม่ต้องกด Trigger
เปิด log ของรอบแรก:

```
logical_date (UTC) : 2026-09-19 17:00:00+00:00
ds                 : 2026-09-19
interval (UTC)     : 2026-09-19 17:00:00+00:00 -> 2026-09-20 17:00:00+00:00
interval (ไทย)     : 2026-09-20 00:00:00+07:00 -> 2026-09-21 00:00:00+07:00
```

⚠️ **เวลาใน Airflow เป็น UTC เสมอ** 🔁 session 4 บท 4.5 ต้อง `.in_timezone("Asia/Bangkok")` ก่อนใช้กับข้อมูลร้านซึ่งเป็นเวลาไทย
⚠️ `ds` = วันที่แบบ UTC → ได้ `2026-09-19` ทั้งที่เป็นรอบของวันที่ 20 (ไทย) **อย่าใช้ `ds` กรองข้อมูลร้าน**

## 4.3 รับ context ได้ 3 แบบ

```python
@task
def a(**context):                       # 1) รับทั้งหมด
    print(context["data_interval_start"])

@task
def b(data_interval_start=None, params=None):   # 2) ตั้งชื่ออาร์กิวเมนต์ให้ตรงกับชื่อใน context
    print(data_interval_start)

@task
def c():
    from airflow.sdk import get_current_context  # 3) ขอเอาเองตรงไหนก็ได้ในขณะที่ task รัน
    context = get_current_context()
```

ค่าที่ใช้บ่อย: `data_interval_start`, `data_interval_end`, `logical_date`, `ds`, `params`, `ti`, `run_id`, `dag_run`

## 4.4 ⌨️ Template `{{ }}` ใน Operator

Operator รับ context ผ่าน **Jinja template** ใน argument ที่รองรับ (เช่น `bash_command`, `sql`)

```python
from airflow.providers.standard.operators.bash import BashOperator

    print_dates = BashOperator(
        task_id="print_dates",
        bash_command=(
            "echo 'ds = {{ ds }}'; "
            "echo 'วันไทย = {{ data_interval_start.in_timezone(\"Asia/Bangkok\").strftime(\"%Y-%m-%d\") }}'; "
            "echo 'เมื่อวานของ ds = {{ macros.ds_add(ds, -1) }}'"
        ),
    )
    show_context() >> print_dates
```

หน้าเว็บ → task → แท็บ **Rendered Templates** ดูว่าแทนค่าแล้วได้คำสั่งจริงอะไร

## 4.5 แบบต่าง ๆ ของ `schedule`

| schedule | ความหมาย |
|---|---|
| `None` | กดรันเองเท่านั้น |
| `"@daily"` หรือ `"0 0 * * *"` | เริ่มรอบเที่ยงคืน (ตาม timezone ของ `start_date`) **ไม่มีช่วง** (`data_interval_start == data_interval_end`) |
| `"@hourly"`, `"@weekly"`, `"@monthly"` | แบบเดียวกับบรรทัดบน |
| `"30 6 * * 1-5"` | 06:30 วันจันทร์–ศุกร์ |
| `CronDataIntervalTimetable("0 0 * * *", timezone="Asia/Bangkok")` | ⭐ มีช่วง `[start, end)` ใช้กับงานดึงข้อมูลรายวัน |
| `timedelta(hours=6)` | ทุก 6 ชั่วโมง |
| `[Asset("...")]` | เมื่อข้อมูลที่รออัปเดต (บทที่ 12) |

⚠️ **Airflow 3 เปลี่ยนจาก Airflow 2:** cron ธรรมดาอย่าง `"@daily"` ไม่มีช่วงแล้ว ดูได้จาก DAG `lesson_04_cron_trigger`
ถ้างานต้องรู้ว่า "ดึงข้อมูลช่วงไหน" ให้ใช้ `CronDataIntervalTimetable`

⚠️ กด **Trigger** เอง (manual) = รอบที่ไม่มีช่วงข้อมูล (`start == end` = เวลาที่กด) ใช้ทดสอบโค้ด แต่ถ้าจะรันข้อมูลวันไหนให้ใช้ backfill

## 4.6 รันย้อนหลังเฉพาะบางวัน (backfill) และรันซ้ำ

หน้าเว็บ: ปุ่ม ▶ Trigger → เลือก **Backfill** → เลือกช่วงวัน

Terminal (ระบุเวลาไทยด้วย `+07:00` ไม่ใส่ = UTC):

```bash
docker compose exec airflow-scheduler airflow backfill create --dag-id my_04_daily \
    --from-date 2026-09-21T00:00:00+07:00 --to-date 2026-09-22T00:00:00+07:00 --reprocess-behavior completed
```

`--reprocess-behavior completed` = วันที่เคยรันสำเร็จแล้วก็รันใหม่ด้วย (ค่าเริ่มต้น `none` = ข้าม)

รันรอบเดิมซ้ำทั้งรอบ: หน้าเว็บ → คลิกคอลัมน์ของ run นั้น → **Clear**

⚠️ อย่าใช้ `airflow dags test <dag_id> <วันที่>` กับ DAG ที่ตั้งเวลา: รอบที่สร้างอาจชนกับรอบตามตาราง ให้ใช้ backfill แทน

## 🧪 ลองเอง

1. เพิ่ม task `@task def build_where_clause()` ที่ใช้ `get_current_context()` สร้างข้อความ
   `WHERE ordered_at >= '2026-09-20 00:00:00' AND ordered_at < '2026-09-21 00:00:00'` (เวลาไทย) ของรอบนั้น แล้ว return
2. สร้าง DAG `my_04_morning` รันทุกวัน 06:30 เวลาไทย จันทร์–ศุกร์ เริ่ม 2026-09-21 ไม่ย้อนหลัง มี task เปล่า 1 ตัว
   แล้วดูในหน้าเว็บว่า **Next Run** เป็นเวลาไหน
3. backfill `my_04_daily` เฉพาะวันที่ 23 ก.ย. ซ้ำ แล้วดูในแท็บ Grid ว่าเกิดอะไรขึ้น

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| รายวันแบบมีช่วง | `schedule=CronDataIntervalTimetable("0 0 * * *", timezone="Asia/Bangkok")` |
| รันย้อนหลังอัตโนมัติ | `start_date` + `catchup=True` (+ `end_date`) |
| ช่วงของรอบ | `data_interval_start`, `data_interval_end` (UTC → `.in_timezone(...)`) |
| ใช้ใน Operator | `{{ data_interval_start }}`, `{{ ds }}`, `{{ macros.ds_add(ds, -1) }}` |
| ย้อนหลังบางวัน | Trigger → Backfill หรือ `airflow backfill create ... +07:00` |
