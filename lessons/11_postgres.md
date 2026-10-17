# บทที่ 11 · ดึงข้อมูลจาก PostgreSQL ⭐

**เป้าหมาย:** ใช้ Operator และ Hook คุยกับฐานข้อมูลร้านค้า โดยดึงเฉพาะข้อมูลของ **ช่วงเวลาของรอบนั้น**

ตัวอย่างเต็ม: `examples/lesson_11_postgres.py` + `examples/sql/*.sql` (อยากรันดู: คัดลอกทั้งไฟล์และโฟลเดอร์ `sql` ลง `dags/my/` README ข้อ 4)

ข้อมูลชุดเดียวกับ MySQL ใน session 3 · ลองเปิด DBeaver ต่อ PostgreSQL (README ข้อ 2) แล้ว `SELECT * FROM orders LIMIT 10`

## 11.1 Operator กับ Hook

| | Operator (`SQLExecuteQueryOperator`) | Hook (`PostgresHook`) |
|---|---|---|
| คืออะไร | task สำเร็จรูป: ให้ SQL มา รันให้ | ตัวต่อฐานข้อมูล ใช้ในโค้ด Python ของเรา |
| ใช้เมื่อ | งานที่เป็น SQL ล้วน (CREATE, INSERT ... SELECT, DELETE) | อยากได้ผลมาทำต่อด้วย Python / pandas |
| ผลลัพธ์ SELECT | ไป XCom (ระวังขนาด) | ได้ในตัวแปรเลย |

ทั้งคู่ใช้ **Connection** (`conn_id` / `postgres_conn_id`) ไม่ต้องเขียน host / รหัสผ่านในโค้ด

## 11.2 ⌨️ SQLExecuteQueryOperator + ช่วงเวลาของรอบ

สร้าง `dags/my/sql/my_daily_sales.sql` (สร้างโฟลเดอร์ `sql` ใน `dags/my/`)

```sql
SELECT sales_channel, count(*) AS orders, sum(total_amount) AS revenue
FROM orders
WHERE ordered_at >= (%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND ordered_at <  (%(end)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND status <> 'cancelled'
GROUP BY sales_channel
ORDER BY sales_channel;
```

สร้าง `dags/my/my_11_postgres.py`

```python
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import CronDataIntervalTimetable, dag, task

BKK = "Asia/Bangkok"
THIS_DAY = {"start": "{{ data_interval_start }}", "end": "{{ data_interval_end }}"}


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=BKK),
    start_date=pendulum.datetime(2026, 9, 20, tz=BKK),
    end_date=pendulum.datetime(2026, 9, 24, tz=BKK),
    catchup=True,
    max_active_runs=1,
    tags=["my"],
)
def my_11_postgres():

    daily_sales = SQLExecuteQueryOperator(
        task_id="daily_sales",
        conn_id="ecommerce_db",
        sql="sql/my_daily_sales.sql",
        parameters=THIS_DAY,
        show_return_value_in_logs=True,
    )

    @task
    def print_sales(rows: list):
        for channel, orders, revenue in rows:
            print(f"{channel:8s} {orders:5d} orders  {revenue:>14,.2f} บาท")

    print_sales(daily_sales.output)


my_11_postgres()
```

เปิดสวิตช์ (unpause) → จะได้ 5 รอบ (20–24 ก.ย.) เปิด log ของ `print_sales` รอบวันที่ 20 ต้องเห็น

```
android    216 orders      205,358.00 บาท
ios        166 orders      222,862.00 บาท
web         81 orders      144,879.00 บาท
```

ทวนกับ DBeaver: `SELECT sales_channel, count(*) FROM orders WHERE ordered_at >= '2026-09-20' AND ordered_at < '2026-09-21' AND status <> 'cancelled' GROUP BY 1;`

| เขียน | ความหมาย |
|---|---|
| `sql="sql/my_daily_sales.sql"` | ไฟล์ .sql นับ path จากโฟลเดอร์ของไฟล์ DAG (หรือใส่ SQL เป็นข้อความตรง ๆ ก็ได้) |
| `parameters={...}` + `%(start)s` ใน SQL | ส่งค่าเข้า SQL อย่างปลอดภัย (ไม่ต่อข้อความเอง) · ค่ามี `{{ }}` ได้ Airflow แทนให้ก่อน |
| `%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok'` | data interval เป็น **UTC** → แปลงเป็น **เวลาไทยไม่มี timezone** ให้ตรงกับคอลัมน์ `ordered_at` 🔁 บทที่ 4 |
| `daily_sales.output` | ผลของ SELECT = list ของแถว 🔁 บทที่ 3 |

⚠️ ไม่ใช่ทุก argument ที่รองรับ `{{ }}`: `SQLCheckOperator` แปลง template ได้แค่ `sql` (ไม่รวม `parameters`) ดูใน `examples/lesson_11_postgres.py`
วิธีเช็ก: ชี้เมาส์ที่ชื่อ Operator ใน VS Code หรือดูหัวข้อ "templated" ในเอกสารของ Operator นั้น

## 11.3 ⌨️ PostgresHook ใน @task

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task
    def top_products(data_interval_start=None, data_interval_end=None) -> str:
        hook = PostgresHook(postgres_conn_id="ecommerce_db")
        df = hook.get_df(
            """
            SELECT p.sku, p.product_name, sum(oi.quantity) AS units, sum(oi.line_total) AS revenue
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.order_id
            JOIN products p ON p.product_id = oi.product_id
            WHERE o.ordered_at >= %(start)s AND o.ordered_at < %(end)s AND o.status <> 'cancelled'
            GROUP BY p.sku, p.product_name
            ORDER BY revenue DESC
            LIMIT 10
            """,
            parameters={
                "start": data_interval_start.in_timezone(BKK).naive(),
                "end": data_interval_end.in_timezone(BKK).naive(),
            },
        )
        day = data_interval_start.in_timezone(BKK).to_date_string()
        path = f"/opt/airflow/output/top_products_{day}.csv"
        df.to_csv(path, index=False)
        return path

    top_products()
```

- แปลงเวลาฝั่ง Python ก็ได้: `.in_timezone(BKK).naive()` = เวลาไทยแบบไม่มี timezone
- 🔁 pandas (session 4 บท 6) · ไฟล์ไปโผล่ที่ `output/` ในเครื่องเรา
- **return แค่ path** ไม่ส่ง DataFrame ผ่าน XCom 🔁 บทที่ 3.4

| method ของ Hook | ได้อะไร |
|---|---|
| `hook.get_first(sql, parameters)` | แถวแรก (tuple) เช่น `hook.get_first("SELECT count(*) FROM orders")[0]` |
| `hook.get_records(sql, parameters)` | ทุกแถว (list ของ tuple) |
| `hook.get_df(sql, parameters)` | pandas DataFrame |
| `hook.run(sql, parameters=...)` | รันคำสั่งที่ไม่ต้องการผล |
| `hook.insert_rows(table, rows, target_fields=[...])` | ใส่หลายแถวจาก Python |

## 11.4 รอข้อมูล + เขียนผล + ตรวจข้อมูล (ดูใน `examples/lesson_11_postgres.py`)

| task | Operator | ทำอะไร |
|---|---|---|
| `wait_for_orders` | `SqlSensor` | รอจน query คืนค่าที่ไม่ใช่ 0 🔁 บทที่ 9 |
| `save_daily_sales` | `SQLExecuteQueryOperator` + `split_statements=True` | CREATE + DELETE + INSERT ลง `lab.daily_sales` ในรายการเดียว |
| `check_saved` | `SQLCheckOperator` | พังถ้าแถวแรกมีค่า 0 / NULL / False |
| `check_columns` | `SQLColumnCheckOperator` | ตรวจเงื่อนไขรายคอลัมน์ (min, null) |

⚠️ ผู้ใช้ `student` **อ่านอย่างเดียว** ใน schema `public` (data source ห้ามเขียนกลับ) เขียนได้เฉพาะ schema `lab`

ดูผลใน DBeaver: `SELECT * FROM lab.daily_sales ORDER BY 1, 2;`

## 🧪 ลองเอง

1. เพิ่ม `SqlSensor` ก่อน `daily_sales` ที่รอจนวันนั้นมี order (ใช้ SQL `count(*)` ช่วงเดียวกัน)
2. เพิ่ม task ด้วย `PostgresHook.get_first` ที่ return จำนวนลูกค้าใหม่ (`customers.registered_at`) ของวันนั้น
3. backfill `my_11_postgres` เฉพาะวันที่ 22 ก.ย. ซ้ำ (บทที่ 4.6) แล้วดูไฟล์ใน `output/`

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| รัน SQL | `SQLExecuteQueryOperator(conn_id=..., sql=..., parameters=...)` |
| SQL จากไฟล์ | `sql="sql/x.sql"` (path จากโฟลเดอร์ของไฟล์ DAG) |
| ผลมาทำต่อด้วย Python | `PostgresHook(...).get_df / get_records / get_first` |
| ช่วงของรอบใน SQL | `%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok'` + `parameters={"start": "{{ data_interval_start }}"}` |
| รอข้อมูล / ตรวจข้อมูล | `SqlSensor` / `SQLCheckOperator`, `SQLColumnCheckOperator` |
