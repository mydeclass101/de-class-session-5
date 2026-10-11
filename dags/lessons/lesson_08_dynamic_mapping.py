"""บทที่ 8 · สร้าง task ตามจำนวนข้อมูล (dynamic task mapping)

ไม่รู้ล่วงหน้าว่าจะมีกี่ไฟล์ / กี่วัน / กี่ตาราง → ให้ Airflow แตก task ตอนรันเอง ด้วย .expand()
- 1 ค่าในลิสต์ = 1 task ย่อย (mapped task) ในหน้า Grid จะเห็นเป็นช่อง [0] [1] [2] ...
- task ถัดไปได้ผลลัพธ์ของทุกช่องรวมเป็นลิสต์ (เหมือน for loop แล้วเก็บผลใส่ list)
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    max_active_tasks=4,                                 # ใน DAG นี้รันพร้อมกันได้ไม่เกิน 4 task
    tags=["session5", "lesson-08"],
)
def lesson_08_dynamic_mapping():

    @task
    def list_days() -> list[str]:
        # ของจริงอาจมาจากการ list ไฟล์ใน bucket หรือ query ฐานข้อมูล
        start = pendulum.date(2026, 9, 20)
        return [start.add(days=i).to_date_string() for i in range(5)]

    # map_index_template: ตั้งชื่อช่องในหน้า Grid ให้อ่านง่าย แทน [0] [1] [2]
    @task(multiple_outputs=False, map_index_template="{{ day }}")
    def count_orders(day: str, shop: str) -> dict:
        get_current_context()["day"] = day              # ค่าที่ template ด้านบนใช้
        total = len(day) * 10                           # สมมติว่านับได้ (บทที่ 11 นับจากฐานข้อมูลจริง)
        print(f"{shop} {day}: {total} orders")
        return {"day": day, "orders": total}

    @task
    def summarize(results: list[dict]):
        print(f"ได้ผลลัพธ์ {len(results)} วัน รวม {sum(r['orders'] for r in results)} orders")

    # partial = ค่าที่ทุกช่องใช้เหมือนกัน · expand = ค่าที่แตกเป็นหลายช่อง
    counts = count_orders.partial(shop="DE Class Shop").expand(day=list_days())
    summarize(counts)

    # expand_kwargs: แต่ละช่องได้อาร์กิวเมนต์หลายตัว (ลิสต์ของ dict)
    @task
    def export_table(table: str, file_format: str):
        print(f"export {table} เป็น {file_format}")

    export_table.expand_kwargs([
        {"table": "orders", "file_format": "parquet"},
        {"table": "customers", "file_format": "csv"},
    ])

    # Operator แบบดั้งเดิมก็ใช้ .partial(...).expand(...) ได้
    BashOperator.partial(task_id="say_table").expand(
        bash_command=[f"echo export {name}" for name in ["orders", "customers", "products"]],
    )


lesson_08_dynamic_mapping()
