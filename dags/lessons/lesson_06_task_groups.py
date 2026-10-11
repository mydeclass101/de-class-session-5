"""บทที่ 6 · จัดกลุ่ม task (TaskGroup) และงานเตรียม/เก็บกวาด (setup / teardown)

- กลุ่มพับ/กางได้ในหน้า Graph task_id จริงจะมีชื่อกลุ่มนำหน้า เช่น extract.orders
- ฟังก์ชันที่สร้างกลุ่มเรียกซ้ำได้: สร้างกลุ่มหน้าตาเดียวกันให้หลายตาราง
- @setup / @teardown: เตรียมของก่อนเริ่ม และเก็บกวาดเสมอแม้งานตรงกลางพัง
"""
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag, setup, task, task_group, teardown

TABLES = ["orders", "customers", "products"]


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-06"],
)
def lesson_06_task_groups():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # แบบที่ 1: with TaskGroup(...) แบบเดียวกับ with DAG(...)
    with TaskGroup(group_id="extract") as extract:
        for table in TABLES:
            EmptyOperator(task_id=table)                      # task_id จริง = extract.orders, ...

    # แบบที่ 2: @task_group ฟังก์ชันที่ return กลุ่ม เรียกซ้ำได้ (ใส่ group_id ไม่ให้ชื่อชนกัน)
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
            clean_table.override(group_id=table)(table)        # clean.orders.remove_duplicates, ...

    # setup / teardown: teardown รันเสมอ (แม้ clean พัง) และ Airflow รู้ว่าเป็นคู่กัน
    @setup
    def create_temp_folder() -> str:
        import os
        path = "/tmp/lesson_06"
        os.makedirs(path, exist_ok=True)
        print("สร้าง", path)
        return path

    @teardown
    def remove_temp_folder(path: str):
        import shutil
        shutil.rmtree(path, ignore_errors=True)
        print("ลบ", path)

    temp = create_temp_folder()
    start >> temp >> extract >> clean >> remove_temp_folder(temp) >> end


lesson_06_task_groups()
