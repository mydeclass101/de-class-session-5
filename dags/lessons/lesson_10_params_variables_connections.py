"""บทที่ 10 · ค่าตั้งค่า 3 แบบ: Params, Variables, Connections

| แบบ        | เปลี่ยนเมื่อไหร่              | ตัวอย่าง                         | ตั้งที่ไหน |
|------------|-----------------------------|----------------------------------|-----------|
| Param      | ทุกครั้งที่กด Trigger         | จำนวนอันดับ, ช่องทางขาย          | ในโค้ด DAG (มีค่าเริ่มต้น) + ฟอร์มตอน Trigger |
| Variable   | นาน ๆ ครั้ง ใช้ร่วมหลาย DAG    | ชื่อร้าน, ค่าตั้งค่ารายงาน         | Admin → Variables / AIRFLOW_VAR_<ชื่อ> |
| Connection | แทบไม่เปลี่ยน มีรหัสผ่าน      | ฐานข้อมูล, API                   | Admin → Connections / AIRFLOW_CONN_<ชื่อ> |

ใน docker-compose.yml ตั้งไว้แล้ว: AIRFLOW_VAR_SHOP_NAME, AIRFLOW_VAR_REPORT_SETTINGS, AIRFLOW_CONN_ECOMMERCE_DB
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import BaseHook, Param, Variable, dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    params={
        "top_n": Param(5, type="integer", minimum=1, maximum=20, description="จำนวนอันดับที่แสดง"),
        "channel": Param("web", enum=["web", "ios", "android"], description="ช่องทางขาย"),
        "note": Param(None, type=["null", "string"], description="หมายเหตุ (ไม่ใส่ก็ได้)"),
    },
    tags=["session5", "lesson-10"],
)
def lesson_10_params_variables_connections():

    @task
    def read_params(params=None):
        print(f"ขอ {params['top_n']} อันดับ ของช่องทาง {params['channel']} หมายเหตุ: {params['note']}")

    @task
    def read_variables():
        # อ่าน Variable "ในฟังก์ชัน" เท่านั้น ถ้าอ่านนอกฟังก์ชัน Airflow จะอ่านทุกครั้งที่ parse ไฟล์ (ทุก 10 วินาที)
        shop = Variable.get("shop_name")
        settings = Variable.get("report_settings", deserialize_json=True)     # ค่าที่เป็น JSON → dict
        currency = Variable.get("currency", default="THB")                    # ไม่มีตัวแปรนี้ → ใช้ค่า default
        print(f"{shop}: top {settings['top_n']}, ขั้นต่ำ {settings['min_order_amount']} {currency}")

    @task
    def read_connection():
        conn = BaseHook.get_connection("ecommerce_db")
        print(f"host={conn.host} port={conn.port} database={conn.schema} user={conn.login}")
        # ⚠️ ห้าม print รหัสผ่าน (conn.password) ลง log: ทุกคนที่เปิดหน้า log ได้จะเห็น

    # Operator อ่านทั้ง 3 แบบผ่าน template
    show_in_bash = BashOperator(
        task_id="show_in_bash",
        bash_command=(
            "echo 'param: {{ params.channel }}'; "
            "echo 'variable: {{ var.value.shop_name }} top {{ var.json.report_settings.top_n }}'; "
            "echo 'connection host: {{ conn.ecommerce_db.host }}'"
        ),
    )

    read_params() >> read_variables() >> read_connection() >> show_in_bash


lesson_10_params_variables_connections()
