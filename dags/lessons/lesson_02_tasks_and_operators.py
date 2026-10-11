"""บทที่ 2 · Task และ Operator: เขียนงานจริงด้วย Python และ Bash

งานเดียวกันเขียนได้ 2 แบบ
- lesson_02_taskflow : แบบใหม่ (TaskFlow)  @dag + @task + @task.bash        ← แนะนำ ใช้เป็นหลัก
- lesson_02_classic  : แบบดั้งเดิม          with DAG(...) + PythonOperator + BashOperator
                       (เจอบ่อยในโค้ดเก่าและเอกสาร ต้องอ่านออก)
"""
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, dag, task

# ค่าตั้งต้นที่ทุก task ใน DAG ได้รับ (task ไหนอยากต่างก็ใส่ทับเองได้)
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


# ---------------------------------------------------------------- แบบ TaskFlow
@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args=DEFAULT_ARGS,
    doc_md="""
### รายงานประจำวัน (แบบ TaskFlow)
ข้อความนี้ไปโผล่ในหน้า DAG บนเว็บ เขียนเป็น **Markdown** ได้
""",
    tags=["session5", "lesson-02"],
)
def lesson_02_taskflow():

    @task
    def greet(name: str, shop: str = "DE Class Shop"):
        print(f"สวัสดี {name} จาก {shop}")

    @task(task_id="check_disk", retries=3)          # ตั้ง task_id เอง + ใส่ retries ทับค่าจาก default_args
    def check_disk_space():
        import shutil                                # import ในฟังก์ชัน: โหลดเฉพาะตอน task รัน
        free_gb = shutil.disk_usage("/").free / 1024**3
        print(f"พื้นที่ว่าง {free_gb:.1f} GB")

    @task.bash
    def list_dags_folder() -> str:
        return "ls -la /opt/airflow/dags/lessons | head -5"     # return คำสั่ง bash ที่จะรัน

    start = EmptyOperator(task_id="start")
    start >> greet("นักเรียน") >> check_disk_space() >> list_dags_folder()


# ---------------------------------------------------------------- แบบดั้งเดิม (Operator)
def greet_function(name: str, shop: str = "DE Class Shop"):
    print(f"สวัสดี {name} จาก {shop}")


with DAG(
    dag_id="lesson_02_classic",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args=DEFAULT_ARGS,
    tags=["session5", "lesson-02"],
):
    start = EmptyOperator(task_id="start")
    greet = PythonOperator(
        task_id="greet",
        python_callable=greet_function,             # ส่งชื่อฟังก์ชัน ไม่มีวงเล็บ (session 4 บท 2.5)
        op_kwargs={"name": "นักเรียน"},
    )
    list_dags_folder = BashOperator(
        task_id="list_dags_folder",
        bash_command="ls -la /opt/airflow/dags/lessons | head -5",
    )
    start >> greet >> list_dags_folder


lesson_02_taskflow()
