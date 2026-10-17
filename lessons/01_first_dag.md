# บทที่ 1 · DAG แรก: วางโครงด้วย task เปล่า

**เป้าหมาย:** เขียน DAG ได้เอง และต่อลำดับ task ได้ทุกแบบ
เริ่มจาก **task เปล่า (`EmptyOperator`)** ที่ไม่ทำอะไรเลย เพื่อวาง "โครง" ของงานให้ถูกก่อน แล้วค่อยใส่งานจริงทีหลัง

ตัวอย่างเต็ม: `examples/lesson_01_first_dag.py` (เปิดอ่านใน VS Code · อยากรันดูให้คัดลอกลง `dags/my/` README ข้อ 4)

## 1.1 ⌨️ สร้างไฟล์ `dags/my/my_01_first_dag.py`

```python
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["my"],
)
def my_01_first_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end


my_01_first_dag()
```

บันทึกไฟล์ (`Ctrl+S`) รอ ~10 วินาที แล้ว refresh หน้า Dags จะเห็น `my_01_first_dag`

| บรรทัด | ความหมาย |
|---|---|
| `@dag(...)` | 🔁 decorator (session 4 บท 2.6): เปลี่ยน function ธรรมดาให้เป็น DAG · **ชื่อ function = `dag_id`** |
| `schedule=None` | ไม่ตั้งเวลา กดรันเองเท่านั้น (บทที่ 4) |
| `start_date=pendulum.datetime(..., tz="Asia/Bangkok")` | DAG เริ่มมีผลตั้งแต่วันไหน **ใส่ timezone เสมอ** |
| `tags=[...]` | ป้ายไว้กรองในหน้าเว็บ |
| `EmptyOperator(task_id="start")` | 🔁 สร้าง object จาก class (session 4 บท 3.3) · `task_id` ห้ามซ้ำใน DAG เดียวกัน |
| `start >> end` | start ก่อน end |
| `my_01_first_dag()` | ⚠️ **ต้องเรียก function ท้ายไฟล์** ไม่งั้น Airflow ไม่เห็น DAG |

💡 `pendulum` = library วันเวลาที่ Airflow ใช้ ทำงานแบบ `datetime` (session 4 บท 4) แต่ใส่ timezone ง่ายกว่า

ลองรัน:

```bash
docker compose exec airflow-scheduler airflow dags test my_01_first_dag
```

## 1.2 ⌨️ วางโครงงานรายงานยอดขาย

แก้ข้างใน function เป็น

```python
    start = EmptyOperator(task_id="start")

    extract_orders = EmptyOperator(task_id="extract_orders")
    extract_customers = EmptyOperator(task_id="extract_customers")
    extract_products = EmptyOperator(task_id="extract_products")

    build_report = EmptyOperator(task_id="build_report")
    send_report = EmptyOperator(task_id="send_report")
    end = EmptyOperator(task_id="end")

    start >> [extract_orders, extract_customers, extract_products] >> build_report
    build_report >> send_report >> end
```

เปิดแท็บ **Graph** ของ `my_01_first_dag` ต้องเห็น start แตกเป็น 3 ทาง แล้วมารวมที่ `build_report`
กด Trigger แล้วดูแท็บ Grid: 3 ตัว `extract_*` รัน **พร้อมกัน**

💡 วางโครงด้วย task เปล่าก่อนเสมอ เห็นลำดับครบแล้วค่อยเปลี่ยนแต่ละกล่องเป็นงานจริง (บทที่ 2)

## 1.3 วิธีเขียนลำดับแบบอื่น

```python
from airflow.sdk import chain, cross_downstream

a >> b              # a ก่อน b
b << a              # เหมือนบรรทัดบน (ลูกศรชี้ไปตัวที่รันทีหลัง)
a >> [b, c]         # b, c หลัง a (รันพร้อมกัน)
[b, c] >> d         # d รอทั้ง b และ c
chain(a, b, c, d)   # = a >> b >> c >> d  อ่านง่ายเมื่อยาว
cross_downstream([a, b], [c, d])   # a→c, a→d, b→c, b→d
```

⚠️ `[a, b] >> [c, d]` **ใช้ไม่ได้** (Python ไม่รู้จัก `>>` ระหว่าง list) ต้องใช้ `cross_downstream`

⚠️ ลำดับที่วนกลับ (`a >> b >> a`) ไม่ได้ เพราะ DAG ห้ามวน (Acyclic) ไฟล์จะขึ้น import error

ดูตัวอย่างทุกแบบใน `lesson_01_dependencies` ของไฟล์ตัวอย่าง (อยากเห็นแท็บ Graph: คัดลอกไฟล์ลง `dags/my/`)

## 1.4 เมื่อไฟล์พัง

⌨️ ลองพิมพ์ผิดโดยตั้งใจ เช่นลบ `)` ตัวหนึ่ง แล้วบันทึก

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

หน้าเว็บจะขึ้นแถบสีแดง **Dag Import Errors** ด้วย 🔁 อ่านบรรทัดสุดท้ายของ error ก่อนเสมอ (session 4 บท 5.1)
แก้กลับแล้วบันทึก error จะหายเอง

## 🧪 ลองเอง

1. เพิ่ม `extract_payments` อีกตัวที่รันพร้อม extract อีก 3 ตัว
2. เพิ่ม `check_quality` ระหว่าง `build_report` กับ `send_report` โดยใช้ `chain(...)`
3. สร้าง DAG ที่ 2 ในไฟล์เดียวกันชื่อ `my_01_weekly` (function ใหม่ + เรียกท้ายไฟล์) มี task `a`, `b`, `c`, `d` ที่ `a`, `b` ต้องเสร็จก่อน `c`, `d` ทุกตัว

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| สร้าง DAG | `@dag(schedule=..., start_date=...)` บน function + เรียก function ท้ายไฟล์ |
| task เปล่า | `EmptyOperator(task_id="...")` |
| ลำดับ | `>>`, `<<`, list, `chain`, `cross_downstream` |
| ทดสอบ | `airflow dags test <dag_id>` · ไฟล์พัง: `airflow dags list-import-errors` |
