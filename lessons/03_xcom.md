# บทที่ 3 · ส่งค่าระหว่าง task (XCom)

**เป้าหมาย:** ส่งผลลัพธ์จาก task หนึ่งไปให้อีก task ได้ และรู้ว่าอะไรไม่ควรส่ง

ตัวอย่างเต็ม: `dags/lessons/lesson_03_xcom.py`

## 3.1 แต่ละ task รันแยกกัน

แต่ละ task รันเป็น **process ใหม่** (ของจริงอาจคนละเครื่อง) ตัวแปรใน task หนึ่ง task อื่นมองไม่เห็น
ถ้าอยากส่งค่า ต้องฝากไว้ที่ Airflow = **XCom** (cross-communication) ซึ่งเก็บใน metadata DB

## 3.2 ⌨️ return แล้วส่งเป็นอาร์กิวเมนต์

สร้าง `dags/my/my_03_xcom.py`

```python
import pendulum
from airflow.sdk import dag, task


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["my"])
def my_03_xcom():

    @task(multiple_outputs=False)
    def get_shop_info() -> dict:
        return {"shop": "DE Class Shop", "currency": "THB"}

    @task(multiple_outputs=True)
    def report_day() -> dict:
        return {"start": "2026-09-20 00:00:00", "end": "2026-09-21 00:00:00"}

    @task
    def make_message(info: dict, start: str, end: str) -> str:
        message = f"{info['shop']}: ยอดขายช่วง [{start}, {end}) หน่วย {info['currency']}"
        print(message)
        return message

    info = get_shop_info()
    day = report_day()
    make_message(info, day["start"], day["end"])


my_03_xcom()
```

- `return` ใน `@task` = เก็บลง XCom ให้อัตโนมัติ
- ส่ง `info` เข้า `make_message(...)` = Airflow **ต่อลำดับให้เอง** (ไม่ต้องเขียน `>>`) และหยิบค่าจาก XCom มาใส่ตอนรัน
- `info` ตอนเขียน DAG **ยังไม่ใช่ dict** เป็นแค่ "ใบรับฝากของ" (`XComArg`) ค่าจริงมาตอนรัน
  จึง `print(info["shop"])` ระดับ DAG ไม่ได้ ต้องทำใน task

| `multiple_outputs` | ผล |
|---|---|
| `False` | dict ทั้งก้อนเป็น XCom 1 ตัว · task ถัดไปรับทั้ง dict |
| `True` | แยกแต่ละ key เป็น XCom · หยิบทีละตัวได้ `day["start"]` |

⚠️ task ที่ return dict **ต้องใส่ `multiple_outputs` ให้ชัดเสมอ** ถ้าไม่ใส่ Airflow จะเดาเองจาก `-> dict` (ruff เตือน)

ดูผล: หน้าเว็บ → task `report_day` → แท็บ **XCom** จะเห็น key `start`, `end` และ `return_value`

## 3.3 ⌨️ Operator แบบดั้งเดิมกับ XCom

เพิ่ม `from airflow.providers.standard.operators.bash import BashOperator` บนสุดของไฟล์ แล้วแก้บรรทัดสุดท้ายใน DAG เป็น

```python
    message = make_message(info, day["start"], day["end"])

    shout = BashOperator(
        task_id="shout",
        bash_command="echo \"{{ ti.xcom_pull(task_ids='make_message') }}\" | tr a-z A-Z",
    )
    message >> shout

    @task
    def show_length(text: str):
        print(f"ข้อความยาว {len(text)} ตัวอักษร")

    show_length(shout.output)
```

| ทิศทาง | เขียน |
|---|---|
| @task → Operator | template `{{ ti.xcom_pull(task_ids='ชื่อ task') }}` ใน argument ที่รองรับ template · ต้องเขียน `>>` เอง |
| Operator → @task | `operator.output` ส่งเป็นอาร์กิวเมนต์ |

`{{ ... }}` = **Jinja template**: Airflow แทนค่าให้ตอนรัน (รายละเอียดบทที่ 4) · `ti` = task instance ของ task ที่กำลังรัน

## 3.4 อะไรควรส่ง / ไม่ควรส่ง

| ✅ ส่งได้ | ❌ ไม่ควรส่ง |
|---|---|
| ตัวเลข ข้อความ dict/list เล็ก ๆ | DataFrame ทั้งตาราง, ข้อมูลหลายพันแถว |
| path ของไฟล์ / ชื่อตาราง ที่เก็บข้อมูลจริง | ไฟล์ทั้งไฟล์ |
| จำนวนแถว, วันที่, สถานะ | รหัสผ่าน (คนเปิดหน้าเว็บเห็น) |

💡 ข้อมูลใหญ่ → task แรกเขียนลงไฟล์หรือตาราง แล้ว **ส่งแค่ path** (บทที่ 11 ทำแบบนี้)

## 🧪 ลองเอง

1. เพิ่ม task `count_words(message: str) -> int` ที่นับจำนวนคำของ `message` แล้ว print
2. เปลี่ยน `get_shop_info` เป็น `multiple_outputs=True` แล้วส่ง `info["shop"]` (แทน `info`) ให้ task ใหม่ `print_shop(shop: str)`
3. เปิดหน้าเว็บดูแท็บ XCom ของ `get_shop_info` ก่อนและหลังข้อ 2 ต่างกันยังไง

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| ส่งค่าออก | `return` |
| รับค่า | ส่งเป็นอาร์กิวเมนต์ `b(a())` |
| dict แยก key | `@task(multiple_outputs=True)` แล้ว `x["key"]` |
| Operator อ่าน XCom | `{{ ti.xcom_pull(task_ids='a') }}` |
| Operator ส่งให้ @task | `op.output` |
