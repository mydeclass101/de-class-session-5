"""บทที่ 5 · เลือกทางเดิน: branch, short_circuit, run_if และ trigger_rule

ลองกด Trigger แล้วเปลี่ยนค่า day / has_new_orders ในฟอร์ม (params ลงรายละเอียดในบทที่ 10)
- day = 2026-09-20 (อาทิตย์) → เดินทาง weekend_report   · day = 2026-09-22 (อังคาร) → weekday_report
- has_new_orders = false     → short_circuit ข้ามงานที่เหลือทั้งหมด (สถานะ skipped สีชมพู)
"""
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Label, Param, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowSkipException


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    params={
        "day": Param("2026-09-20", type="string", format="date", description="วันที่ทำรายงาน"),
        "has_new_orders": Param(True, type="boolean", description="วันนั้นมี order ใหม่ไหม"),
    },
    tags=["session5", "lesson-05"],
)
def lesson_05_branching():

    # 1) @task.short_circuit: คืน False = ข้าม (skip) ทุก task ที่อยู่ถัดไป, True = ไปต่อ
    @task.short_circuit
    def has_new_orders(params=None) -> bool:
        print("มี order ใหม่:", params["has_new_orders"])
        return params["has_new_orders"]

    # 2) @task.branch: คืน task_id (หรือ list ของ task_id) ที่จะให้รันต่อ ทางอื่นถูก skip
    @task.branch
    def pick_report(params=None) -> str:
        day = pendulum.parse(params["day"])
        return "weekend_report" if day.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY) else "weekday_report"

    weekday_report = EmptyOperator(task_id="weekday_report")
    weekend_report = EmptyOperator(task_id="weekend_report")

    # 3) trigger_rule: ค่าเริ่มต้นคือ all_success (ทุกตัวก่อนหน้าต้องสำเร็จ)
    #    หลัง branch จะมีทางหนึ่งถูก skip เสมอ จึงต้องใช้ none_failed_min_one_success ไม่อย่างนั้นจะถูก skip ตาม
    publish = EmptyOperator(task_id="publish", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # 4) @task.run_if: เงื่อนไขของ task ตัวเดียว (False = skip เฉพาะตัวนี้ ตัวถัดไปไม่โดน)
    @task.run_if(lambda context: context["params"]["day"].endswith("-01"))
    @task
    def monthly_summary():
        print("วันที่ 1 ของเดือน: ทำสรุปรายเดือนด้วย")

    # 5) skip จากข้างในด้วย AirflowSkipException
    @task
    def notify_marketing(params=None):
        if pendulum.parse(params["day"]).day_of_week != pendulum.MONDAY:
            raise AirflowSkipException("ทีมการตลาดขอรายงานเฉพาะวันจันทร์")
        print("ส่งรายงานให้ทีมการตลาด")

    # 6) all_done: รันเสมอไม่ว่าตัวก่อนหน้าจะสำเร็จ พัง หรือถูก skip (เช่น งานเก็บกวาด)
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup():
        print("เก็บกวาดไฟล์ชั่วคราว")

    branch = pick_report()
    has_new_orders() >> branch
    branch >> Label("จันทร์–ศุกร์") >> weekday_report >> publish     # Label = ป้ายบนเส้นในหน้า Graph
    branch >> Label("เสาร์–อาทิตย์") >> weekend_report >> publish
    publish >> [monthly_summary(), notify_marketing()] >> cleanup()


lesson_05_branching()
