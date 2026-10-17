# บทที่ 6 · จัดกลุ่ม task และงานเตรียม/เก็บกวาด

**เป้าหมาย:** DAG ที่มี task หลายสิบตัวยังอ่านง่าย และสร้างกลุ่มหน้าตาเดียวกันซ้ำได้

ตัวอย่างเต็ม: `examples/lesson_06_task_groups.py`

## 6.1 ⌨️ `TaskGroup` แบบ `with`

สร้าง `dags/my/my_06_groups.py`

```python
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag, task, task_group

TABLES = ["orders", "customers", "products"]


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["my"])
def my_06_groups():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="extract") as extract:
        for table in TABLES:
            EmptyOperator(task_id=table)

    start >> extract >> end


my_06_groups()
```

- 🔁 `for` สร้าง task ทีละตาราง (session 4 บท 1.8) · task_id จริงคือ `extract.orders`, `extract.customers`, ...
- `start >> extract` = start ก่อน **ทุกตัว** ในกลุ่ม · `extract >> end` = end รอ **ทุกตัว** ในกลุ่ม
- หน้า Graph: กลุ่มพับอยู่ คลิกเพื่อกางดูข้างใน

## 6.2 ⌨️ `@task_group` = กลุ่มที่เรียกซ้ำได้

```python
    @task_group
    def clean_table(table: str):
        @task
        def remove_duplicates():
            print(f"ตัดแถวซ้ำของ {table}")

        @task
        def fix_types():
            print(f"แปลงชนิดข้อมูลของ {table}")

        remove_duplicates() >> fix_types()

    with TaskGroup(group_id="clean") as clean:
        for table in TABLES:
            clean_table.override(group_id=table)(table)

    start >> extract >> clean >> end
```

- `@task_group` บน function = function ที่สร้างกลุ่ม เรียกกี่ครั้งก็ได้กลุ่มใหม่
- `.override(group_id=table)` ตั้งชื่อกลุ่มไม่ให้ซ้ำ → `clean.orders.remove_duplicates`
- กลุ่มซ้อนกลุ่มได้

## 6.3 `@setup` / `@teardown`: เตรียมของ และเก็บกวาดเสมอ

```python
from airflow.sdk import setup, teardown

    @setup
    def create_temp_folder() -> str:
        import os
        os.makedirs("/tmp/my_06", exist_ok=True)
        return "/tmp/my_06"

    @teardown
    def remove_temp_folder(path: str):
        import shutil
        shutil.rmtree(path, ignore_errors=True)

    temp = create_temp_folder()
    start >> temp >> extract >> clean >> remove_temp_folder(temp) >> end
```

- teardown **รันเสมอ** แม้งานตรงกลางพัง (เหมือน `finally` 🔁 session 4 บท 5.2)
- ต่างจาก `trigger_rule=ALL_DONE` ตรงที่ Airflow รู้ว่าเป็นคู่กัน: ถ้า Clear task ตรงกลางเพื่อรันใหม่ setup/teardown จะรันใหม่ให้ด้วย
- หน้า Graph แสดงเส้นประระหว่าง setup กับ teardown

## 🧪 ลองเอง

1. เพิ่มตาราง `payments` ใน `TABLES` แล้วดูว่ากลุ่ม extract และ clean เพิ่มเองหรือไม่
2. เพิ่ม task `validate` ในกลุ่ม `clean_table` ต่อจาก `fix_types`
3. แก้ `fix_types` ให้ `raise ValueError("ลองพัง")` แล้วดูว่า `remove_temp_folder` ยังรันไหม และ `end` เป็นสถานะอะไร

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| จัดกลุ่ม | `with TaskGroup(group_id="x"):` |
| กลุ่มซ้ำได้ | `@task_group` + `.override(group_id=...)` |
| เตรียม / เก็บกวาดเสมอ | `@setup` / `@teardown` |
