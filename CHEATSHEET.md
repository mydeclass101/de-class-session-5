# Airflow 3.3 Cheatsheet

## โครง DAG

```python
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task, chain, Param, Variable, CronDataIntervalTimetable, TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    schedule=None,                                           # None = กดรันเอง
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    catchup=False,                                           # ค่าเริ่มต้นของ Airflow 3
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"top_n": Param(5, type="integer")},
    tags=["demo"],
)
def my_dag():                                                # ชื่อฟังก์ชัน = dag_id

    @task
    def extract() -> list[int]:
        return [1, 2, 3]                                     # return = XCom

    @task
    def load(rows: list[int]):
        print(len(rows))

    start = EmptyOperator(task_id="start")
    start >> load(extract())                                 # ส่งค่า = ต่อลำดับให้เอง

my_dag()                                                     # ⚠️ ต้องเรียกฟังก์ชันท้ายไฟล์ ไม่งั้นไม่เกิด DAG
```

## ลำดับ task

| เขียน | ความหมาย |
|---|---|
| `a >> b` / `b << a` | a ก่อน b |
| `a >> [b, c] >> d` | b, c รันพร้อมกันหลัง a, d รอทั้งคู่ |
| `chain(a, b, c, d)` | ต่อกันเป็นแถว |
| `cross_downstream([a, b], [c, d])` | ทุกตัวซ้าย → ทุกตัวขวา |
| `a >> Label("ข้อความ") >> b` | ป้ายบนเส้นในหน้า Graph |

## Decorator ของ task

| Decorator | ใช้เมื่อ |
|---|---|
| `@task` | ฟังก์ชัน Python ธรรมดา |
| `@task(task_id=..., retries=..., multiple_outputs=True)` | ตั้งค่าเฉพาะ task |
| `@task.bash` | return เป็นคำสั่ง bash |
| `@task.branch` | return `task_id` (หรือ list) ที่จะไปต่อ ทางอื่นถูก skip |
| `@task.short_circuit` | return `False` = ข้ามทุก task ที่ตามมา |
| `@task.run_if(lambda context: ...)` / `@task.skip_if(...)` | เงื่อนไขของ task ตัวนั้นตัวเดียว |
| `@task.sensor(poke_interval=30, timeout=600, mode="reschedule")` | return `PokeReturnValue(is_done=..., xcom_value=...)` |
| `@task_group` / `with TaskGroup("ชื่อ"):` | จัดกลุ่ม |
| `@setup` / `@teardown` | เตรียมของ / เก็บกวาดเสมอ |
| `task.partial(a=1).expand(b=[...])` | แตก task ตามจำนวนข้อมูล |
| `@asset(schedule=...)` | 1 ฟังก์ชัน = DAG ที่สร้าง Asset 1 ตัว |

## Operator ที่ใช้บ่อย

| Operator | import จาก |
|---|---|
| `EmptyOperator` | `airflow.providers.standard.operators.empty` |
| `BashOperator` | `airflow.providers.standard.operators.bash` |
| `PythonOperator` | `airflow.providers.standard.operators.python` |
| `TriggerDagRunOperator` | `airflow.providers.standard.operators.trigger_dagrun` |
| `TimeDeltaSensor` | `airflow.providers.standard.sensors.time_delta` |
| `SQLExecuteQueryOperator`, `SQLCheckOperator`, `SQLColumnCheckOperator` | `airflow.providers.common.sql.operators.sql` |
| `SqlSensor` | `airflow.providers.common.sql.sensors.sql` |
| `PostgresHook` | `airflow.providers.postgres.hooks.postgres` |
| `AirflowSkipException`, `AirflowFailException` | `airflow.sdk.exceptions` |

## เวลา

| schedule | ความหมาย |
|---|---|
| `None` | รันเมื่อกดเท่านั้น |
| `"@daily"`, `"0 6 * * *"` | เริ่มรอบตามเวลา **ไม่มีช่วง** (`data_interval_start == data_interval_end`) |
| `CronDataIntervalTimetable("0 0 * * *", timezone="Asia/Bangkok")` | รอบละ 1 วัน มีช่วง `[start, end)` ⭐ ใช้กับงานดึงข้อมูลรายวัน |
| `timedelta(hours=6)` | ทุก 6 ชั่วโมง |
| `[Asset("ชื่อ")]` | เมื่อ Asset นั้นอัปเดต |
| `CronPartitionTimetable("0 1 * * *", timezone="Asia/Bangkok", run_offset=-1, key_format="%Y-%m-%d")` | ตาม cron + ทุก run มีป้ายวัน (partition key) ของเมื่อวาน |
| `PartitionedAssetTimetable(assets=a & b)` | 1 run ต่อ 1 ป้าย เมื่อ a และ b มีป้ายเดียวกันครบ · ป้ายอยู่ใน `context["partition_key"]` |

| ค่าใน context | ตัวอย่าง (รอบของวันที่ 20 ก.ย. เวลาไทย) |
|---|---|
| `data_interval_start` | `2026-09-19 17:00:00+00:00` (= 20 ก.ย. 00:00 ไทย) |
| `data_interval_end` | `2026-09-20 17:00:00+00:00` |
| `logical_date` | = `data_interval_start` (รอบตามตารางเวลา) |
| `ds` | `2026-09-19` ⚠️ วันที่แบบ **UTC** |
| `params`, `ti`, `run_id`, `dag_run` | |

ทุกเวลาใน context เป็น **UTC** → `.in_timezone("Asia/Bangkok")` · ใน SQL: `'{{ data_interval_start }}'::timestamptz AT TIME ZONE 'Asia/Bangkok'`

รับ context ได้ 3 แบบ: `def f(**context)` · `def f(params=None, ti=None)` · `get_current_context()`
ใน Operator ใช้ template: `{{ ds }}`, `{{ data_interval_start }}`, `{{ params.x }}`, `{{ var.value.x }}`, `{{ conn.x.host }}`, `{{ ti.xcom_pull(task_ids='a') }}`, `{{ macros.ds_add(ds, -1) }}`

## Trigger rule (`trigger_rule=TriggerRule.XXX`)

| ค่า | รันเมื่อ |
|---|---|
| `ALL_SUCCESS` (ค่าเริ่มต้น) | ทุกตัวก่อนหน้าสำเร็จ |
| `NONE_FAILED_MIN_ONE_SUCCESS` | ไม่มีตัวไหนพัง และสำเร็จอย่างน้อย 1 (ใช้หลัง branch) |
| `ALL_DONE` | ทุกตัวจบแล้ว ไม่สนผล (งานเก็บกวาด) |
| `ONE_FAILED` | มีตัวไหนพังสักตัว (แจ้งเตือน) |
| `NONE_FAILED` | ไม่มีตัวไหนพัง (skip ได้) |

## เมื่อพัง

```python
@task(retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
      execution_timeout=timedelta(minutes=10), on_failure_callback=notify)
```
- `raise AirflowFailException("...")` = พังเลย ไม่ลองใหม่ (ข้อมูลผิด)
- `raise AirflowSkipException("...")` = ข้าม task นี้
- หน้าเว็บ: คลิก task → **Clear** = รันใหม่ · **Mark as success / failed**

## ค่าตั้งค่า

```python
Variable.get("shop_name")                                   # อ่านในฟังก์ชันเท่านั้น
Variable.get("report_settings", deserialize_json=True)
Variable.get("currency", default="THB")
BaseHook.get_connection("ecommerce_db").host                # from airflow.sdk import BaseHook
```
env: `AIRFLOW_VAR_SHOP_NAME=...` · `AIRFLOW_CONN_ECOMMERCE_DB=postgresql://user:pass@host:5432/db`

## PostgreSQL

```python
SQLExecuteQueryOperator(task_id="q", conn_id="ecommerce_db", sql="sql/daily.sql",
                        parameters={"start": "{{ data_interval_start }}"})       # ใน SQL: %(start)s
hook = PostgresHook(postgres_conn_id="ecommerce_db")
hook.get_first(sql)      # แถวแรก (tuple)
hook.get_records(sql)    # ทุกแถว (list ของ tuple)
hook.get_df(sql)         # pandas DataFrame
hook.run(sql)            # คำสั่งที่ไม่คืนผล (CREATE / INSERT / DELETE)
```

## Terminal

```bash
docker compose exec airflow-scheduler airflow dags test <dag_id>
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags trigger <dag_id> -c '{"top_n": 3}'
docker compose exec airflow-scheduler airflow backfill create --dag-id <dag_id> \
    --from-date 2026-09-21T00:00:00+07:00 --to-date 2026-09-22T00:00:00+07:00 --reprocess-behavior completed
docker compose exec airflow-scheduler airflow tasks test <dag_id> <task_id>     # รัน task เดียว
```

## เจอโค้ดเก่า (Airflow 2) ในเน็ตหรือจาก ChatGPT → เขียนแบบ Airflow 3

ตรวจไฟล์ตัวเองได้ด้วย `ruff check --select AIR --preview dags/my`

| ❌ แบบเก่า | ✅ Airflow 3 |
|---|---|
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` |
| `from airflow.models import Variable` | `from airflow.sdk import Variable` |
| `from airflow.hooks.base import BaseHook` | `from airflow.sdk import BaseHook` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `DummyOperator` | `EmptyOperator` |
| `PostgresOperator` | `SQLExecuteQueryOperator(conn_id=...)` |
| `schedule_interval="@daily"` | `schedule=...` |
| `days_ago(1)` | `pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok")` |
| `execution_date`, `{{ execution_date }}` | `logical_date` / `data_interval_start` |
| `next_ds`, `prev_ds`, `tomorrow_ds` | `data_interval_end`, `{{ macros.ds_add(ds, 1) }}` |
| `provide_context=True` | ไม่ต้องใส่ (ได้ context เสมอ) |
| `Dataset("...")` | `Asset("...")` |
| `SubDagOperator` | `TaskGroup` |
| `sla=...` | ถูกถอดออก (ใช้ `DeadlineAlert` แทน) |
| `catchup` เปิดเป็นค่าเริ่มต้น | ปิดเป็นค่าเริ่มต้น ต้องใส่ `catchup=True` เอง |
| `airflow webserver` | `airflow api-server` (+ ต้องมี `airflow dag-processor`) |
| `airflow dags backfill` | `airflow backfill create` |
| task ต่อ metadata DB เอง (`settings.Session`) | ทำไม่ได้แล้ว ใช้ API / `Variable` / `Connection` แทน |
