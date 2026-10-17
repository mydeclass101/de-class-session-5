"""บทที่ 1 · DAG แรก: วางโครง pipeline ด้วย task เปล่า (EmptyOperator) และลำดับ >>

ไฟล์นี้มี 2 DAG
- lesson_01_first_dag       : โครงงานรายงานยอดขายรายวัน (ยังไม่มีงานจริง มีแต่กล่องเปล่า)
- lesson_01_dependencies    : วิธีเขียนลำดับแบบอื่น ๆ (<<, chain, cross_downstream)
"""
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import chain, cross_downstream, dag


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    description="โครงงานรายงานยอดขายรายวันของร้าน",
    tags=["session5", "lesson-01"],
)
def lesson_01_first_dag():
    start = EmptyOperator(task_id="start")

    extract_orders = EmptyOperator(task_id="extract_orders")
    extract_customers = EmptyOperator(task_id="extract_customers")
    extract_products = EmptyOperator(task_id="extract_products")

    build_report = EmptyOperator(task_id="build_report")
    send_report = EmptyOperator(task_id="send_report")
    end = EmptyOperator(task_id="end")

    # start ต้องเสร็จก่อน แล้ว extract 3 ตัวรันพร้อมกัน ครบทั้ง 3 แล้วค่อย build_report
    start >> [extract_orders, extract_customers, extract_products] >> build_report
    build_report >> send_report >> end


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-01"],
)
def lesson_01_dependencies():
    a, b, c, d = (EmptyOperator(task_id=name) for name in ["a", "b", "c", "d"])

    # 1) >> กับ << : ลูกศรชี้ไปทางที่รันทีหลัง  (a >> b เหมือน b << a)
    a >> b
    c << b

    # 2) chain: ต่อกันเป็นแถวยาว อ่านง่ายเมื่อมีหลายตัว
    step_1, step_2, step_3, step_4 = (EmptyOperator(task_id=f"step_{i}") for i in range(1, 5))
    chain(step_1, step_2, step_3, step_4)                 # = step_1 >> step_2 >> step_3 >> step_4

    # 3) list >> list ใช้ไม่ได้ ต้องใช้ cross_downstream (ทุกตัวซ้าย -> ทุกตัวขวา)
    left = [EmptyOperator(task_id=f"left_{i}") for i in range(1, 3)]
    right = [EmptyOperator(task_id=f"right_{i}") for i in range(1, 3)]
    cross_downstream(left, right)

    d >> step_1


lesson_01_first_dag()
lesson_01_dependencies()
