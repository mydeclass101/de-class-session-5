# บทที่ 5 · เลือกทางเดิน: branch, short_circuit, run_if, trigger_rule ⭐

**เป้าหมาย:** ให้ DAG ตัดสินใจเองว่าจะรัน task ไหน ข้ามอะไร และ task ไหนต้องรันเสมอ

ตัวอย่างเต็ม: `examples/lesson_05_branching.py` (คัดลอกลง `dags/my/` แล้วกด Trigger เปลี่ยนค่าในฟอร์มดูได้)

## 5.1 ⌨️ `@task.branch` = เลือกทาง

สร้าง `dags/my/my_05_branching.py`

```python
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Label, Param, TriggerRule, dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    params={"day": Param("2026-09-20", type="string", format="date")},
    tags=["my"],
)
def my_05_branching():

    @task.branch
    def pick_report(params=None) -> str:
        day = pendulum.parse(params["day"])
        if day.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY):
            return "weekend_report"
        return "weekday_report"

    weekday_report = EmptyOperator(task_id="weekday_report")
    weekend_report = EmptyOperator(task_id="weekend_report")
    publish = EmptyOperator(task_id="publish")

    branch = pick_report()
    branch >> Label("จันทร์–ศุกร์") >> weekday_report >> publish
    branch >> Label("เสาร์–อาทิตย์") >> weekend_report >> publish


my_05_branching()
```

- `params={"day": Param(...)}` = ค่าที่เลือกได้ตอนกด Trigger (ลงลึกบทที่ 10) · task รับด้วย `params=None`
- `@task.branch` **return `task_id`** ของทางที่จะไป (หรือ list ของ task_id) ทางอื่นทั้งหมดถูก **skip**
- `Label("...")` = ป้ายบนเส้นในหน้า Graph อ่านง่ายขึ้น

กด Trigger ด้วยค่าเริ่มต้น (20 ก.ย. = วันอาทิตย์) ดู Grid → `weekday_report` ถูก skip (ชมพู) และ... **`publish` ถูก skip ด้วย!**

## 5.2 trigger_rule: task นี้จะรันเมื่อไร

ค่าเริ่มต้น `all_success` = **ทุก** task ก่อนหน้าต้องสำเร็จ `publish` มี `weekday_report` ที่ถูก skip จึงถูก skip ตาม

⌨️ แก้เป็น

```python
    publish = EmptyOperator(task_id="publish", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
```

| `TriggerRule.` | รันเมื่อ | ใช้กับ |
|---|---|---|
| `ALL_SUCCESS` (ค่าเริ่มต้น) | ทุกตัวก่อนหน้าสำเร็จ | งานปกติ |
| `NONE_FAILED_MIN_ONE_SUCCESS` | ไม่มีตัวไหนพัง + สำเร็จอย่างน้อย 1 | ⭐ จุดรวมหลัง branch |
| `ALL_DONE` | ทุกตัวจบ ไม่สนผล | งานเก็บกวาด |
| `ONE_FAILED` | มีตัวไหนพังสักตัว | แจ้งเตือน (บทที่ 7) |
| `NONE_FAILED` | ไม่มีตัวไหนพัง (skip ได้) | |
| `ALL_FAILED`, `ALL_SKIPPED`, `ONE_SUCCESS`, `ALWAYS` | ตามชื่อ | ใช้น้อย |

## 5.3 ⌨️ `@task.short_circuit` = ไม่มีอะไรให้ทำก็หยุด

```python
    @task.short_circuit
    def has_new_orders(params=None) -> bool:
        return params["has_new_orders"]
```

เพิ่ม `"has_new_orders": Param(True, type="boolean")` ใน `params` แล้วต่อ `has_new_orders() >> branch`

- return `True` = ไปต่อ · `False` = **skip ทุก task ที่ตามมาทั้งหมด** (รวมตัวที่ตั้ง trigger_rule ไว้ด้วย)
- ใช้กับ "วันนี้ไม่มีไฟล์ใหม่ / ไม่มีข้อมูลใหม่ ก็ไม่ต้องทำต่อ" (skip ไม่ใช่ fail: ไม่มีอะไรผิด)

## 5.4 skip เฉพาะ task เดียว

```python
from airflow.sdk.exceptions import AirflowSkipException

    @task.run_if(lambda context: context["params"]["day"].endswith("-01"))
    @task
    def monthly_summary():
        print("วันที่ 1: ทำสรุปรายเดือน")

    @task
    def notify_marketing(params=None):
        if pendulum.parse(params["day"]).day_of_week != pendulum.MONDAY:
            raise AirflowSkipException("ส่งเฉพาะวันจันทร์")
        print("ส่งรายงานให้ทีมการตลาด")
```

| วิธี | ต่างกันยังไง |
|---|---|
| `@task.run_if(เงื่อนไข)` / `@task.skip_if(เงื่อนไข)` | เช็กก่อนรัน เงื่อนไขเป็น function รับ `context` · ตัวถัดไปไม่โดน skip ตาม (ถ้าตั้ง trigger_rule) |
| `raise AirflowSkipException(...)` | ตัดสินใจระหว่างรันในโค้ด พร้อมเหตุผลใน log |
| `@task.short_circuit` | skip **ทุกตัวที่ตามมา** |
| `@task.branch` | เลือกทาง ทางที่ไม่เลือกถูก skip |

💡 `lambda context: ...` = function สั้น ๆ 1 บรรทัด 🔁 session 4 บท 2.7

## 🧪 ลองเอง

1. เพิ่ม task `cleanup` ต่อท้ายทุกอย่าง ที่ **รันเสมอ** ไม่ว่าจะเกิดอะไรก่อนหน้า
2. Trigger ด้วย `has_new_orders = false` แล้วดูว่า `cleanup` รันไหม ทำไม (อ่าน 5.3 อีกรอบ)
3. เพิ่ม `month_start_report`: ถ้าเป็นวันที่ 1 ของเดือน ให้ branch ไป `month_start_report` **เพิ่ม** จากทางปกติ (return list ของ task_id)

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| เลือกทาง | `@task.branch` return task_id + จุดรวมใช้ `NONE_FAILED_MIN_ONE_SUCCESS` |
| ไม่มีงานก็หยุดทั้งหมด | `@task.short_circuit` return False |
| ข้าม task เดียว | `@task.run_if(...)` หรือ `raise AirflowSkipException(...)` |
| รันเสมอ | `trigger_rule=TriggerRule.ALL_DONE` |
