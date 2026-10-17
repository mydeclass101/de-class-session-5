# บทที่ 2 · Task และ Operator: ใส่งานจริงลงในกล่อง

**เป้าหมาย:** เปลี่ยน task เปล่าเป็นงานจริงด้วย Python และ Bash และอ่านโค้ดแบบดั้งเดิม (`with DAG` + Operator) ออก

ตัวอย่างเต็ม: `examples/lesson_02_tasks_and_operators.py`

## 2.1 ⌨️ `@task` = function Python 1 ตัวเป็น task 1 กล่อง

สร้าง `dags/my/my_02_tasks.py`

```python
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    default_args={"owner": "ชื่อเรา", "retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["my"],
)
def my_02_tasks():

    @task
    def greet(name: str, shop: str = "DE Class Shop"):
        print(f"สวัสดี {name} จาก {shop}")

    @task(task_id="check_disk", retries=3)
    def check_disk_space():
        import shutil
        free_gb = shutil.disk_usage("/").free / 1024**3
        print(f"พื้นที่ว่าง {free_gb:.1f} GB")

    start = EmptyOperator(task_id="start")
    start >> greet("นักเรียน") >> check_disk_space()


my_02_tasks()
```

| เขียน | ความหมาย |
|---|---|
| `@task` | function นี้คือ task · `task_id` = ชื่อ function |
| `@task(task_id=..., retries=3)` | 🔁 keyword argument (session 4 บท 2.2) ตั้งค่าเฉพาะ task นี้ |
| `greet("นักเรียน")` | **ไม่ได้รัน function ตอนนี้** แค่สร้างกล่องใน DAG พร้อมจดค่าที่จะส่งเข้าไป · ของจริงรันตอน task รัน |
| `default_args={...}` | ค่าตั้งต้นที่ทุก task ได้รับ task ไหนใส่เองจะใช้ของตัวเอง (`check_disk` ได้ retries=3) |
| `import shutil` ในฟังก์ชัน | library ที่ใช้เฉพาะตอนรัน import ข้างในจะทำให้ไฟล์ parse เร็ว |

```bash
docker compose exec airflow-scheduler airflow dags test my_02_tasks
```

ต้องเห็น `สวัสดี นักเรียน จาก DE Class Shop`

⚠️ print ไปอยู่ใน **Logs** ของ task (หน้าเว็บ) ไม่ใช่หน้าจอใครสักคน

## 2.2 ⌨️ `@task.bash` = รันคำสั่ง bash

เพิ่มใน DAG

```python
    @task.bash
    def list_dags_folder() -> str:
        return "ls -la /opt/airflow/dags/my | head -5"
```

และต่อท้ายลำดับ: `start >> greet("นักเรียน") >> check_disk_space() >> list_dags_folder()`

💡 function **return ข้อความคำสั่ง** แล้ว Airflow เอาไปรันใน bash ของคอนเทนเนอร์ (Linux) · บรรทัดสุดท้ายที่ echo ออกมาจะเป็นผลลัพธ์ (XCom บทที่ 3)

## 2.3 ⌨️ แบบดั้งเดิม: `with DAG(...)` + Operator

โค้ดใน Airflow รุ่นก่อน ๆ และเอกสารจำนวนมากเขียนแบบนี้ ต้องอ่านออก งานเดียวกับ 2.1
**พิมพ์ต่อท้ายไฟล์ `my_02_tasks.py` เดิม** (1 ไฟล์มีหลาย DAG ได้) ย้าย `import` ไปไว้บนสุดของไฟล์:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def greet_function(name: str, shop: str = "DE Class Shop"):
    print(f"สวัสดี {name} จาก {shop}")


with DAG(
    dag_id="my_02_classic",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["my"],
):
    start = EmptyOperator(task_id="start")
    greet = PythonOperator(
        task_id="greet",
        python_callable=greet_function,
        op_kwargs={"name": "นักเรียน"},
    )
    list_dags_folder = BashOperator(
        task_id="list_dags_folder",
        bash_command="ls -la /opt/airflow/dags/my | head -5",
    )
    start >> greet >> list_dags_folder
```

| TaskFlow (แนะนำ) | แบบดั้งเดิม |
|---|---|
| `@dag` บน function + เรียก function | `with DAG(dag_id=...):` 🔁 `with` (session 4 บท 3.5) |
| `@task` | `PythonOperator(python_callable=greet_function)` 🔁 ส่ง function ไม่มีวงเล็บ (session 4 บท 2.5) |
| อาร์กิวเมนต์ `greet("นักเรียน")` | `op_kwargs={"name": "นักเรียน"}` |
| `@task.bash` | `BashOperator(bash_command=...)` |

💡 **ผสมกันได้**: ใน `@dag` ใช้ Operator ได้ทุกตัว (เราใช้ `EmptyOperator` มาตั้งแต่บทที่ 1) งานที่มี Operator สำเร็จรูป (SQL, Bash, ส่ง Slack ...) ใช้ Operator งาน Python ของเราเองใช้ `@task`

## 2.4 ตั้งค่าที่ระดับ DAG ที่ใช้บ่อย

```python
@dag(
    dag_id="my_report",                 # ตั้งชื่อเองได้ (ไม่ใส่ = ชื่อ function)
    description="รายงานยอดขาย",          # คำอธิบายสั้นในหน้า Dags
    doc_md="### อธิบายยาว ๆ เป็น Markdown",  # แสดงในหน้า DAG
    default_args={"owner": "data-team", "retries": 2},
    max_active_runs=1,                  # รันพร้อมกันได้กี่รอบ
    max_active_tasks=8,                 # task ใน DAG นี้รันพร้อมกันได้กี่ตัว
    tags=["sales"],
    ...
)
```

## 🧪 ลองเอง

1. เพิ่ม task `@task def count_letters(word: str)` ที่ print ว่าคำนั้นมีกี่ตัวอักษร แล้วต่อท้าย DAG `my_02_tasks` โดยส่ง `"airflow"`
2. ใน `my_02_classic` เพิ่ม `BashOperator` ชื่อ `show_date` ที่รัน `date` แล้วต่อท้าย `list_dags_folder`
3. ใน `my_02_tasks` ทำให้ `greet` ลองใหม่ได้ 5 ครั้งโดยไม่แก้ `default_args`

## ✅ สรุป

| อยาก | TaskFlow | แบบดั้งเดิม |
|---|---|---|
| รัน Python | `@task` | `PythonOperator(python_callable=f, op_kwargs={...})` |
| รัน bash | `@task.bash` (return คำสั่ง) | `BashOperator(bash_command="...")` |
| ตั้งค่าทุก task | `default_args={...}` | `default_args={...}` |
| ตั้งค่า task เดียว | `@task(retries=3)` | `PythonOperator(..., retries=3)` |
