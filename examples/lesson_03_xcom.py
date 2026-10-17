"""บทที่ 3 · ส่งข้อมูลระหว่าง task (XCom)

- return ค่าจาก @task = เก็บลง XCom ให้ task ถัดไปหยิบใช้
- ส่งเป็นอาร์กิวเมนต์ = Airflow ต่อลำดับ (>>) ให้เอง
- XCom เก็บใน metadata DB: ส่งได้แค่ของเล็ก ๆ (dict, ตัวเลข, path ของไฟล์) ห้ามส่งข้อมูลทั้งตาราง
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["session5", "lesson-03"],
)
def lesson_03_xcom():

    # task ที่ return dict ต้องบอกเสมอว่าจะแยก key ไหม: False = เก็บทั้ง dict เป็นก้อนเดียว
    @task(multiple_outputs=False)
    def get_shop_info() -> dict:
        return {"shop": "DE Class Shop", "currency": "THB", "vat_rate": 0.07}

    @task(multiple_outputs=True)                  # แยก key ของ dict ให้หยิบทีละตัวได้: report_day["start"]
    def report_day() -> dict:
        return {"start": "2026-09-20 00:00:00", "end": "2026-09-21 00:00:00"}

    @task
    def make_message(info: dict, start: str, end: str) -> str:
        message = f"{info['shop']}: ยอดขายช่วง [{start}, {end}) หน่วย {info['currency']}"
        print(message)
        return message

    info = get_shop_info()
    day = report_day()
    message = make_message(info, day["start"], day["end"])     # ไม่ต้องเขียน >> เอง Airflow รู้จากอาร์กิวเมนต์

    # Operator แบบดั้งเดิมหยิบ XCom ผ่าน template {{ ti.xcom_pull(...) }}
    shout = BashOperator(
        task_id="shout",
        bash_command="echo \"{{ ti.xcom_pull(task_ids='make_message') }}\" | tr a-z A-Z",
    )
    message >> shout

    # และ Operator ก็ส่งต่อให้ @task ได้: .output = ค่าที่ Operator นั้น push ลง XCom (บรรทัดสุดท้ายของ bash)
    @task
    def show_length(text: str):
        print(f"ข้อความยาว {len(text)} ตัวอักษร")

    show_length(shout.output)


lesson_03_xcom()
