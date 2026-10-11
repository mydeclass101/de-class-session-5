# บทที่ 8 · สร้าง task ตามจำนวนข้อมูล (dynamic task mapping)

**เป้าหมาย:** ไม่รู้ล่วงหน้าว่าจะมีกี่ไฟล์ / กี่วัน / กี่ตาราง ก็ให้ Airflow แตก task ตอนรันได้

ตัวอย่างเต็ม: `dags/lessons/lesson_08_dynamic_mapping.py`

## 8.1 ทำไมไม่ใช้ `for`

บทที่ 6 ใช้ `for table in TABLES` ได้เพราะ **รู้รายการตอนเขียนโค้ด**
แต่ "ไฟล์ที่ partner ส่งมาวันนี้" รู้ตอน **รัน** เท่านั้น → ใช้ `.expand()`

## 8.2 ⌨️ `.expand()`

สร้าง `dags/my/my_08_mapping.py`

```python
import pendulum
from airflow.sdk import dag, task


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["my"])
def my_08_mapping():

    @task
    def list_days() -> list[str]:
        start = pendulum.date(2026, 9, 20)
        return [start.add(days=i).to_date_string() for i in range(5)]

    @task
    def count_orders(day: str, shop: str) -> int:
        print(f"{shop} {day}")
        return len(day)

    @task
    def summarize(results: list[int]):
        print(f"ได้ {len(results)} ค่า รวม {sum(results)}")

    counts = count_orders.partial(shop="DE Class Shop").expand(day=list_days())
    summarize(counts)


my_08_mapping()
```

| เขียน | ความหมาย |
|---|---|
| `.expand(day=list_days())` | 1 ค่าในลิสต์ = 1 task ย่อย (mapped task) |
| `.partial(shop=...)` | ค่าที่ทุก task ย่อยใช้เหมือนกัน |
| `summarize(counts)` | task ถัดไปได้ผลของทุกช่องรวมเป็น list |

หน้าเว็บ: ช่องของ `count_orders` มีวงเล็บ `[5]` = แตกเป็น 5 ตัว คลิกเข้าไปจะเห็น `[0]` ถึง `[4]`

## 8.3 ตั้งชื่อช่อง + จำกัดจำนวนที่รันพร้อมกัน

```python
from airflow.sdk import get_current_context

    @task(map_index_template="{{ day }}", max_active_tis_per_dag=2)
    def count_orders(day: str, shop: str) -> int:
        get_current_context()["day"] = day          # ค่าที่ template ใช้
        ...
```

- `map_index_template` = ชื่อที่แสดงแทน `[0]`, `[1]`
- `max_active_tis_per_dag=2` = task นี้รันพร้อมกันไม่เกิน 2 ช่อง (ไม่ถล่มฐานข้อมูล)

## 8.4 แบบอื่น

```python
# หลายอาร์กิวเมนต์ต่อช่อง: ลิสต์ของ dict
export_table.expand_kwargs([
    {"table": "orders", "file_format": "parquet"},
    {"table": "customers", "file_format": "csv"},
])

# Operator แบบดั้งเดิม
BashOperator.partial(task_id="say_table").expand(
    bash_command=["echo orders", "echo customers"],
)
```

⚠️ `.expand()` ต้องได้ list (หรือ dict) · ถ้าได้ list ว่าง task นั้นถูก skip

## 🧪 ลองเอง

1. เปลี่ยน `list_days` ให้คืน 10 วัน แล้วดูหน้า Grid
2. ใส่ `map_index_template` ให้ช่องแสดงเป็นวันที่
3. เขียน `@task def list_tables() -> list[str]` ที่คืน `["orders", "customers", "products"]` แล้ว map task `export_table(table: str)` ด้วย `.expand(table=list_tables())`

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| แตก task ตามรายการที่รู้ตอนรัน | `t.expand(x=list_task())` |
| ค่าที่เหมือนกันทุกช่อง | `t.partial(y=...).expand(x=...)` |
| หลายอาร์กิวเมนต์ต่อช่อง | `t.expand_kwargs([{...}, {...}])` |
| รวมผล | ส่งผลของ mapped task ให้ task ถัดไป = list |
