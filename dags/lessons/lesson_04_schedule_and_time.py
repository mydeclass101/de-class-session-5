"""บทที่ 4 · ตั้งเวลา และช่วงเวลาของแต่ละรอบ (data interval)

- lesson_04_daily_interval : รันวันละรอบ เที่ยงคืนเวลาไทย แต่ละรอบรู้ช่วง [start, end) ของตัวเอง
                             start_date / end_date + catchup=True = รันย้อนหลังให้ครบทุกวัน 20–24 ก.ย. (5 รอบ)
- lesson_04_cron_trigger   : schedule="@daily" แบบสั้น ๆ ใน Airflow 3 จะ "ไม่มีช่วง" (start = end)
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import CronDataIntervalTimetable, dag, get_current_context, task

BKK = "Asia/Bangkok"


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=BKK),   # ทุกวัน 00:00 เวลาไทย, ครอบ 1 วันเต็ม
    start_date=pendulum.datetime(2026, 9, 20, tz=BKK),
    end_date=pendulum.datetime(2026, 9, 24, tz=BKK),            # รอบสุดท้าย = วันที่ 24 (ข้อมูลร้านมีถึงวันนี้)
    catchup=True,                       # รอบที่ผ่านไปแล้ว (20–24 ก.ย.) ให้รันย้อนหลังด้วย (ค่าเริ่มต้นของ Airflow 3 = False)
    max_active_runs=1,                  # รันทีละรอบ เรียงวัน
    tags=["session5", "lesson-04"],
)
def lesson_04_daily_interval():

    @task
    def show_context(**context):
        # context = ข้อมูลของรอบนี้ที่ Airflow ส่งมาให้ (รับด้วย **context แบบ **kwargs ใน session 4 บท 2.4)
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        print("run_id             :", context["run_id"])
        print("logical_date (UTC) :", context["logical_date"])
        print("ds                 :", context["ds"], " <- วันที่ของ logical_date ตามเวลา UTC")
        print("interval (UTC)     :", start, "->", end)
        print("interval (ไทย)     :", start.in_timezone(BKK), "->", end.in_timezone(BKK))

    @task
    def build_where_clause() -> str:
        # อยู่ในฟังก์ชันลึก ๆ ที่ไม่ได้รับ **context ก็ขอ context ได้ด้วย get_current_context()
        context = get_current_context()
        start = context["data_interval_start"].in_timezone(BKK).strftime("%Y-%m-%d %H:%M:%S")
        end = context["data_interval_end"].in_timezone(BKK).strftime("%Y-%m-%d %H:%M:%S")
        where = f"WHERE ordered_at >= '{start}' AND ordered_at < '{end}'"      # [start, end) session 4 บท 4.3
        print(where)
        return where

    # Operator อ่าน context ผ่าน Jinja template {{ ... }} ใน argument ที่รองรับ (เช่น bash_command)
    print_dates = BashOperator(
        task_id="print_dates",
        bash_command=(
            "echo 'ds = {{ ds }}'; "
            "echo 'วันไทย = {{ data_interval_start.in_timezone(\"Asia/Bangkok\").strftime(\"%Y-%m-%d\") }}'; "
            "echo 'เมื่อวานของ ds = {{ macros.ds_add(ds, -1) }}'"
        ),
    )

    show_context() >> build_where_clause() >> print_dates


@dag(
    schedule="@daily",                  # = cron "0 0 * * *" แบบ trigger: เริ่มรอบตามเวลา แต่ไม่มีช่วงข้อมูล
    start_date=pendulum.datetime(2026, 9, 20, tz=BKK),
    catchup=False,                      # ค่าเริ่มต้น: เปิดแล้วรันแค่รอบล่าสุด ไม่ย้อนหลัง
    tags=["session5", "lesson-04"],
)
def lesson_04_cron_trigger():

    @task
    def show_interval(data_interval_start=None, data_interval_end=None):
        # ขอเฉพาะค่าที่ต้องใช้ได้ด้วยการตั้งชื่ออาร์กิวเมนต์ให้ตรงกับชื่อใน context
        print("start =", data_interval_start)
        print("end   =", data_interval_end, " <- เท่ากับ start: ไม่มีช่วง")

    show_interval()


lesson_04_daily_interval()
lesson_04_cron_trigger()
