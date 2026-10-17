# บทที่ 10 · ค่าตั้งค่า: Params, Variables, Connections

**เป้าหมาย:** ไม่เขียนค่าที่เปลี่ยนได้หรือรหัสผ่านลงในโค้ดตรง ๆ

ตัวอย่างเต็ม: `examples/lesson_10_params_variables_connections.py`

🔁 session 4 บท 5.6: "รหัสผ่านห้ามอยู่ในโค้ด โค้ดขึ้น git รหัสก็หลุด" ใน Airflow ใช้ Connection แทน `.env`

| แบบ | เปลี่ยนเมื่อไหร่ | ตัวอย่าง | ตั้งที่ไหน |
|---|---|---|---|
| **Param** | ทุกครั้งที่กด Trigger | จำนวนอันดับ, ช่องทางขาย | ในโค้ด DAG (มีค่าเริ่มต้น) + ฟอร์มตอน Trigger |
| **Variable** | นาน ๆ ครั้ง ใช้ร่วมหลาย DAG | ชื่อร้าน, ค่าตั้งค่ารายงาน | Admin → Variables หรือ env `AIRFLOW_VAR_<ชื่อ>` |
| **Connection** | แทบไม่เปลี่ยน มีรหัสผ่าน | ฐานข้อมูล, API | Admin → Connections หรือ env `AIRFLOW_CONN_<ชื่อ>` |

## 10.1 ⌨️ Params

สร้าง `dags/my/my_10_settings.py`

```python
import pendulum
from airflow.sdk import BaseHook, Param, Variable, dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    params={
        "top_n": Param(5, type="integer", minimum=1, maximum=20, description="จำนวนอันดับ"),
        "channel": Param("web", enum=["web", "ios", "android"]),
        "note": Param(None, type=["null", "string"]),
    },
    tags=["my"],
)
def my_10_settings():

    @task
    def read_params(params=None):
        print(f"ขอ {params['top_n']} อันดับ ของช่องทาง {params['channel']} หมายเหตุ: {params['note']}")

    read_params()


my_10_settings()
```

กด ▶ Trigger → จะมี **ฟอร์ม** ให้กรอกตามที่ประกาศไว้ (ตัวเลข 1–20, ตัวเลือก web/ios/android) กรอกผิดฟอร์มจะเตือน
จาก Terminal: `airflow dags trigger my_10_settings -c '{"top_n": 3, "channel": "ios"}'`

ใน Operator: `{{ params.top_n }}`

## 10.2 ⌨️ Variables

`docker-compose.yml` ตั้งไว้ให้แล้ว 2 ตัว: `AIRFLOW_VAR_SHOP_NAME`, `AIRFLOW_VAR_REPORT_SETTINGS` (ชื่อ Variable = ส่วนหลัง `AIRFLOW_VAR_` ตัวพิมพ์เล็ก)

```python
    @task
    def read_variables():
        shop = Variable.get("shop_name")
        settings = Variable.get("report_settings", deserialize_json=True)
        currency = Variable.get("currency", default="THB")
        print(f"{shop}: top {settings['top_n']} {currency}")
```

- `deserialize_json=True` = ค่าเป็น JSON → dict 🔁 session 4 บท 3.6
- `default=` = ไม่มี Variable นี้ใช้ค่านี้ (ไม่ใส่แล้วไม่มี = error)
- ใน Operator: `{{ var.value.shop_name }}`, `{{ var.json.report_settings.top_n }}`

🧪 หน้าเว็บ → Admin → Variables → **Add** ชื่อ `currency` ค่า `USD` แล้วรันใหม่ ดูว่าผลเปลี่ยนไหม

⚠️ **อ่าน Variable ในฟังก์ชัน task เท่านั้น** ถ้าเขียน `shop = Variable.get(...)` นอกฟังก์ชัน Airflow จะอ่านทุกครั้งที่ parse ไฟล์ (ทุก ~10 วินาที)

## 10.3 ⌨️ Connections

`docker-compose.yml` ตั้ง `AIRFLOW_CONN_ECOMMERCE_DB=postgresql://student:student123@ecommerce-db:5432/ecommerce` ไว้แล้ว
= Connection ชื่อ `ecommerce_db` (รูปแบบ URI: `ชนิด://user:password@host:port/database`)

```python
    @task
    def read_connection():
        conn = BaseHook.get_connection("ecommerce_db")
        print(f"host={conn.host} port={conn.port} database={conn.schema} user={conn.login}")
```

⚠️ ห้าม print `conn.password` ลง log ทุกคนที่เปิดหน้า log ได้จะเห็น
💡 ส่วนใหญ่ไม่ต้องอ่าน Connection เอง แค่ส่ง **ชื่อ** ให้ Operator/Hook (`conn_id="ecommerce_db"`) บทที่ 11
💡 host คือ `ecommerce-db` (ชื่อ service ใน docker compose) ไม่ใช่ `localhost` เพราะ Airflow อยู่ในคอนเทนเนอร์ 🔁 session 2

### สร้าง Connection ในหน้าเว็บ

Admin → Connections → **Add Connection**

| ช่อง | ค่า |
|---|---|
| Connection Id | `ecommerce_db_ui` |
| Connection Type | Postgres |
| Host / Port | `ecommerce-db` / `5432` |
| Database | `ecommerce` |
| Login / Password | `student` / `student123` |

กด **Test** → ต้องขึ้นว่าสำเร็จ แล้วบันทึก

⚠️ Connection/Variable ที่ตั้งผ่าน environment variable **ไม่แสดงในหน้า Admin** (แต่ใช้ได้) ดูได้จาก Terminal:

```bash
docker compose exec airflow-scheduler airflow connections get ecommerce_db
docker compose exec airflow-scheduler airflow variables get shop_name
```

## 🧪 ลองเอง

1. เพิ่ม Param `min_amount` (ตัวเลข ≥ 0 ค่าเริ่มต้น 1000) แล้ว print ใน `read_params`
2. เปลี่ยน `read_connection` ให้ใช้ `ecommerce_db_ui` ที่สร้างในหน้าเว็บ
3. เพิ่ม `BashOperator` ที่ echo `{{ params.channel }}` และ `{{ var.value.shop_name }}`

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| ค่าที่เลือกตอน Trigger | `params={"x": Param(...)}` · `params["x"]` · `{{ params.x }}` |
| ค่าตั้งค่าร่วม | `Variable.get("x", default=...)` · `{{ var.value.x }}` |
| ฐานข้อมูล / รหัสผ่าน | Connection + `conn_id="..."` |
