"""บทที่ 11 · ดึงข้อมูลจาก PostgreSQL ด้วย Operator และ Hook

ฐานข้อมูล ecommerce (ชุดเดียวกับ MySQL ใน session 3) ต่อผ่าน Connection ชื่อ ecommerce_db
- SqlSensor               : รอจนกว่าวันนั้นจะมี order
- SQLExecuteQueryOperator : รัน SQL จากไฟล์ .sql ผลลัพธ์ (SELECT) ไปอยู่ใน XCom
- PostgresHook ใน @task    : ดึงเป็น pandas DataFrame แล้วทำอะไรต่อด้วย Python ก็ได้
- SQLCheckOperator / SQLColumnCheckOperator : ตรวจข้อมูล ผิด = task พัง

รันวันละรอบ (เวลาไทย) ย้อนหลัง 20–24 ก.ย. ทุกรอบดึงเฉพาะ order ในช่วง [data_interval_start, data_interval_end)
"""
import pendulum
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import CronDataIntervalTimetable, dag, task

BKK = "Asia/Bangkok"
CONN_ID = "ecommerce_db"
# ค่าที่ส่งเข้า SQL ทุกไฟล์: Airflow แทน {{ ... }} ด้วยช่วงเวลาของรอบนั้นก่อนรัน
THIS_DAY = {"start": "{{ data_interval_start }}", "end": "{{ data_interval_end }}"}


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=BKK),
    start_date=pendulum.datetime(2026, 9, 20, tz=BKK),
    end_date=pendulum.datetime(2026, 9, 24, tz=BKK),
    catchup=True,
    max_active_runs=1,
    tags=["session5", "lesson-11"],
)
def lesson_11_postgres():

    # 1) รอให้มีข้อมูลก่อน (ค่าแรกของแถวแรก = 0 → รอต่อ, ไม่ใช่ 0 → ผ่าน)
    wait_for_orders = SqlSensor(
        task_id="wait_for_orders",
        conn_id=CONN_ID,
        sql="sql/count_orders.sql",                    # path นับจากโฟลเดอร์ของไฟล์ DAG นี้
        parameters=THIS_DAY,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    # 2) SELECT ด้วย Operator: ผลลัพธ์ = list ของแถว → XCom
    daily_sales = SQLExecuteQueryOperator(
        task_id="daily_sales",
        conn_id=CONN_ID,
        sql="sql/daily_sales.sql",
        parameters=THIS_DAY,
        show_return_value_in_logs=True,                # แสดงผล query ใน log
    )

    @task
    def print_sales(rows: list):
        for channel, orders, revenue in rows:
            print(f"{channel:8s} {orders:5d} orders  {revenue:>14,.2f} บาท")

    # 3) Hook = ตัวต่อฐานข้อมูลที่ใช้ใน Python ได้เอง ใช้เมื่ออยากทำต่อด้วย pandas / Python
    @task
    def top_products(data_interval_start=None, data_interval_end=None) -> str:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
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
                # แปลงฝั่ง Python ก็ได้: เวลา UTC → เวลาไทย → ตัด timezone ทิ้งให้ตรงกับคอลัมน์
                "start": data_interval_start.in_timezone(BKK).naive(),
                "end": data_interval_end.in_timezone(BKK).naive(),
            },
        )
        day = data_interval_start.in_timezone(BKK).to_date_string()
        path = f"/opt/airflow/output/top_products_{day}.csv"      # = output/ ในเครื่องเรา
        df.to_csv(path, index=False)
        print(df.head(5).to_string())
        return path                                    # ส่งต่อแค่ path ไม่ส่งทั้งตาราง

    # 4) เขียนผลลงตารางใน schema lab (หลายคำสั่งในไฟล์เดียว รันใน transaction เดียว)
    save_daily_sales = SQLExecuteQueryOperator(
        task_id="save_daily_sales",
        conn_id=CONN_ID,
        sql="sql/save_daily_sales.sql",
        parameters=THIS_DAY,
        split_statements=True,
    )

    # 5) ตรวจข้อมูล: SQLCheckOperator พังถ้ามีค่าไหนในแถวแรกเป็น 0 / NULL / False
    #    ⚠️ operator นี้ไม่แปลง {{ }} ใน parameters (template ได้เฉพาะ sql) จึงใส่ template ลงใน SQL ตรง ๆ
    check_saved = SQLCheckOperator(
        task_id="check_saved",
        conn_id=CONN_ID,
        sql="""SELECT count(*) FROM lab.daily_sales
               WHERE sales_date = ('{{ data_interval_start }}'::timestamptz AT TIME ZONE 'Asia/Bangkok')::date""",
    )

    check_columns = SQLColumnCheckOperator(
        task_id="check_columns",
        conn_id=CONN_ID,
        table="lab.daily_sales",
        column_mapping={
            "orders": {"min": {"greater_than": 0}},
            "revenue": {"null_check": {"equal_to": 0}, "min": {"geq_to": 0}},
        },
    )

    wait_for_orders >> [daily_sales, top_products()]
    print_sales(daily_sales.output)
    daily_sales >> save_daily_sales >> [check_saved, check_columns]


lesson_11_postgres()
