"""บทที่ 12 · ให้ DAG ต่อกัน: Asset (รอข้อมูลพร้อม) และ TriggerDagRunOperator (สั่ง DAG อื่น)

- lesson_12_producer        : ทำตาราง daily_sales เสร็จ → ประกาศว่า Asset "daily_sales" อัปเดตแล้ว
- lesson_12_consumer        : schedule=[asset] รันเองทุกครั้งที่ producer อัปเดต Asset (ไม่ต้องตั้งเวลา)
- lesson_12_trigger_another : สั่ง DAG อื่นให้รันตรง ๆ (เหมาะกับ "ทำ A เสร็จแล้วสั่ง B" แบบชัด ๆ)
- daily_sales_report        : @asset แบบย่อ 1 ฟังก์ชัน = 1 DAG ที่สร้าง Asset 1 ตัว
- lesson_12_partition_sales / lesson_12_partition_customers : CronPartitionTimetable ทุกรอบติดป้ายวัน (partition key) เช่น "2026-09-20"
- lesson_12_partition_dashboard : PartitionedAssetTimetable รันวันละ 1 รอบ เมื่อ "วันเดียวกัน" ของทั้ง 2 Asset มาครบ

ลองเอง: เปิด (unpause) consumer ไว้ แล้วกด Trigger producer → consumer จะรันตามเอง
ลองเอง (partition): unpause lesson_12_partition_dashboard แล้วกด Trigger sales ใส่ Partition key 2026-09-20,
customers ใส่ 2026-09-21 → dashboard ยังไม่รัน (คนละวัน) · Trigger customers อีกครั้งใส่ 2026-09-20 → dashboard รันของวันที่ 20
⚠️ lesson_12_trigger_another: ต้อง unpause lesson_00_setup_check ก่อน ไม่งั้น run ที่ถูกสั่งค้าง queued และ task นี้รอไม่จบ
"""
import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, CronPartitionTimetable, PartitionedAssetTimetable, asset, dag, task

daily_sales = Asset("daily_sales")                    # ชื่อของข้อมูล ใช้ร่วมกันหลาย DAG


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["session5", "lesson-12"])
def lesson_12_producer():

    @task(outlets=[daily_sales])                        # task นี้สำเร็จ = Asset อัปเดต
    def build_daily_sales():
        print("สร้างตาราง daily_sales เสร็จ")

    build_daily_sales()


@dag(
    schedule=[daily_sales],                             # รันเมื่อ Asset นี้อัปเดต
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-12"],
)
def lesson_12_consumer():

    @task
    def send_dashboard(**context):
        for event in context["triggering_asset_events"][daily_sales]:
            print("ข้อมูลอัปเดตจาก", event.source_dag_id, "เมื่อ", event.timestamp)

    send_dashboard()


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["session5", "lesson-12"])
def lesson_12_trigger_another():
    TriggerDagRunOperator(
        task_id="run_setup_check",
        trigger_dag_id="lesson_00_setup_check",
        wait_for_completion=True,                       # รอจน DAG นั้นจบก่อน task นี้จึงสำเร็จ
        poke_interval=5,
    )


@asset(schedule=None, uri="daily_sales_report")        # 1 ฟังก์ชัน = DAG ชื่อ daily_sales_report + Asset ชื่อเดียวกัน
def daily_sales_report():
    print("ทำรายงานสรุปยอดขาย")


# ---------- Asset partition: แยกสัญญาณเป็นรายวัน ----------
sales_by_day = Asset("daily_sales_by_day")
customers_by_day = Asset("daily_customers_by_day")


@dag(
    schedule=CronPartitionTimetable(
        "0 1 * * *",                                    # รันตี 1 เวลาไทยทุกวัน
        timezone="Asia/Bangkok",
        run_offset=-1,                                  # ทำข้อมูลของ "รอบก่อนหน้า" = เมื่อวาน
        key_format="%Y-%m-%d",                          # ป้ายวัน เช่น "2026-09-20"
    ),
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-12"],
)
def lesson_12_partition_sales():

    @task(outlets=[sales_by_day])                       # Asset event ได้ป้ายวันเดียวกับ run นี้
    def build_sales(**context):
        print("ทำ daily_sales ของวันที่", context["partition_key"])

    build_sales()


@dag(
    schedule=CronPartitionTimetable("0 3 * * *", timezone="Asia/Bangkok", run_offset=-1, key_format="%Y-%m-%d"),
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-12"],
)
def lesson_12_partition_customers():

    @task(outlets=[customers_by_day])
    def build_customers(**context):
        print("ทำ daily_customers ของวันที่", context["partition_key"])

    build_customers()


@dag(
    schedule=PartitionedAssetTimetable(assets=sales_by_day & customers_by_day),   # ป้ายวันต้องตรงกันทั้ง 2 ตัว
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-12"],
)
def lesson_12_partition_dashboard():

    @task
    def send_dashboard(**context):
        print("ส่ง dashboard ของวันที่", context["partition_key"])

    send_dashboard()


lesson_12_producer()
lesson_12_consumer()
lesson_12_trigger_another()
lesson_12_partition_sales()
lesson_12_partition_customers()
lesson_12_partition_dashboard()
