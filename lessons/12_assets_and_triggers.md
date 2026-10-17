# บทที่ 12 · ให้ DAG ต่อกัน: Asset และ TriggerDagRunOperator

**เป้าหมาย:** DAG หนึ่งรันเมื่ออีก DAG ทำข้อมูลเสร็จ โดยไม่ต้องเดาเวลา และรอข้อมูล "วันเดียวกัน" ครบก่อนรัน (partition)

ตัวอย่างเต็ม: `examples/lesson_12_assets_and_triggers.py`

## 12.1 ปัญหา

ทีม A ทำตาราง `daily_sales` เสร็จราว ๆ ตี 1 ทีม B ตั้ง DAG dashboard ไว้ตี 2 "เผื่อ"
วันไหน A ช้า B ก็ได้ข้อมูลเก่า → ให้ B **รอสัญญาณ** ว่าข้อมูลพร้อมแทนการเดาเวลา

## 12.2 ⌨️ Asset

สร้าง `dags/my/my_12_assets.py`

```python
import pendulum
from airflow.sdk import Asset, dag, task

my_sales = Asset("my_daily_sales")


@dag(schedule=None, start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["my"])
def my_12_producer():

    @task(outlets=[my_sales])
    def build_daily_sales():
        print("สร้างตาราง daily_sales เสร็จ")

    build_daily_sales()


@dag(schedule=[my_sales], start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"), tags=["my"])
def my_12_consumer():

    @task
    def send_dashboard(**context):
        for event in context["triggering_asset_events"][my_sales]:
            print("ข้อมูลอัปเดตจาก", event.source_dag_id)

    send_dashboard()


my_12_producer()
my_12_consumer()
```

1. เปิดสวิตช์ทั้ง 2 DAG
2. กด Trigger `my_12_producer`
3. `my_12_consumer` รันตามเองทันทีที่ producer เสร็จ · เมนู **Assets** ในหน้าเว็บแสดงเส้นเชื่อม

| เขียน | ความหมาย |
|---|---|
| `Asset("ชื่อ")` | ชื่อของข้อมูลชิ้นหนึ่ง (ตาราง, ไฟล์) ที่หลาย DAG ใช้ร่วมกัน |
| `@task(outlets=[asset])` | task นี้สำเร็จ = ประกาศว่า asset อัปเดตแล้ว |
| `schedule=[asset]` | รันเมื่อ asset อัปเดต · หลายตัว `[a, b]` = รอให้อัปเดตครบทุกตัว |

💡 Airflow ไม่ได้ตรวจข้อมูลจริง Asset เป็นแค่ **สัญญาณ** ที่ task ของเราประกาศ

## 12.3 `@asset` แบบย่อ

```python
from airflow.sdk import asset


@asset(schedule=None, uri="daily_sales_report")
def daily_sales_report():
    print("ทำรายงานสรุปยอดขาย")
```

1 function = DAG ชื่อเดียวกับ function + Asset ที่ DAG นั้นสร้าง (เหมาะกับงานที่ "สร้างข้อมูล 1 ชิ้น")

## 12.4 สั่ง DAG อื่นตรง ๆ: TriggerDagRunOperator

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    TriggerDagRunOperator(
        task_id="run_setup_check",
        trigger_dag_id="lesson_00_setup_check",
        wait_for_completion=True,
        poke_interval=5,
    )
```

| | Asset | TriggerDagRunOperator |
|---|---|---|
| ใครรู้จักใคร | ไม่ต้องรู้จักกัน รู้แค่ชื่อข้อมูล | ต้นทางต้องรู้ชื่อ DAG ปลายทาง |
| ปลายทางหลายตัว | เพิ่ม consumer ได้เรื่อย ๆ ไม่ต้องแก้ producer | ต้องแก้ต้นทางทุกครั้ง |
| ใช้เมื่อ | ⭐ "ข้อมูลพร้อมแล้ว ใครรอก็ไปต่อ" | "ทำ A เสร็จ สั่ง B ทันที" แบบชัด ๆ, ส่ง `conf` ให้ปลายทาง |

⚠️ DAG ปลายทางต้อง **unpause** อยู่ ไม่งั้น run ที่ถูกสั่งจะค้างที่ `queued`

## 12.5 ⌨️ Asset partition: ให้สัญญาณมี "ป้ายวัน"

`schedule=[sales, customers]` ใน 12.2 จำแค่ว่า "แต่ละ Asset อัปเดตแล้วหรือยัง" **ไม่รู้ว่าเป็นข้อมูลวันไหน**

| เหตุการณ์ | `schedule=[sales, customers]` |
|---|---|
| sales ของวันที่ 21 เสร็จ แต่ customers ยังเป็นของวันที่ 20 | consumer รันเลย ได้ข้อมูล **คนละวัน** มาผสมกัน |
| customers พังวันที่ 20 แล้วกลับมาวันที่ 21 | สัญญาณ sales วันที่ 20, 21 **ค้างรอ** แล้วถูกรวมเป็น run เดียว consumer ไม่รู้ว่าทำของวันไหน |

Asset partition แก้ปัญหานี้: สัญญาณทุกครั้งมีป้าย (**partition key**) เช่น `"2026-09-20"`
consumer รัน **1 ครั้งต่อ 1 ป้าย** และรันเมื่อทุก Asset มีป้ายวันเดียวกันครบแล้ว

เพิ่มต่อท้าย `dags/my/my_12_assets.py` (บรรทัด import แก้ของเดิม)

```python
from airflow.sdk import Asset, CronPartitionTimetable, PartitionedAssetTimetable, dag, task

my_sales_by_day = Asset("my_sales_by_day")
my_customers_by_day = Asset("my_customers_by_day")


@dag(
    schedule=CronPartitionTimetable("0 1 * * *", timezone="Asia/Bangkok", run_offset=-1, key_format="%Y-%m-%d"),
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["my"],
)
def my_12_sales_by_day():

    @task(outlets=[my_sales_by_day])
    def build_sales(**context):
        print("ทำ daily_sales ของวันที่", context["partition_key"])

    build_sales()


@dag(
    schedule=CronPartitionTimetable("0 3 * * *", timezone="Asia/Bangkok", run_offset=-1, key_format="%Y-%m-%d"),
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["my"],
)
def my_12_customers_by_day():

    @task(outlets=[my_customers_by_day])
    def build_customers(**context):
        print("ทำ daily_customers ของวันที่", context["partition_key"])

    build_customers()


@dag(
    schedule=PartitionedAssetTimetable(assets=my_sales_by_day & my_customers_by_day),
    start_date=pendulum.datetime(2026, 9, 20, tz="Asia/Bangkok"),
    tags=["my"],
)
def my_12_dashboard_by_day():

    @task
    def send_dashboard(**context):
        print("ส่ง dashboard ของวันที่", context["partition_key"])

    send_dashboard()


my_12_sales_by_day()
my_12_customers_by_day()
my_12_dashboard_by_day()
```

| เขียน | ความหมาย |
|---|---|
| `CronPartitionTimetable("0 1 * * *", timezone="Asia/Bangkok")` | รันตาม cron เหมือนเดิม แต่ทุก run มีป้าย partition key |
| `run_offset=-1` | ป้ายเป็นของ **รอบก่อนหน้า** · run ตี 1 วันที่ 21 → ป้าย `2026-09-20` (ทำข้อมูลเมื่อวาน) |
| `key_format="%Y-%m-%d"` | รูปแบบป้าย (ถ้าไม่ใส่ = `2026-09-20T01:00:00`) |
| `@task(outlets=[asset])` | เหมือนเดิม · สัญญาณได้ป้ายเดียวกับ run เอง ไม่ต้องเขียนเพิ่ม |
| `PartitionedAssetTimetable(assets=a & b)` | รอ a **และ** b ที่ป้ายเดียวกัน แยกนับทีละป้าย |
| `context["partition_key"]` | ป้ายของ run นี้ เช่น `"2026-09-20"` |

ลอง:
1. unpause `my_12_dashboard_by_day` **ก่อน** แล้วค่อย unpause อีก 2 ตัว
   → producer ทั้งคู่รัน 1 ครั้งของ "เมื่อวาน" → dashboard รันตามด้วยป้ายเมื่อวาน
2. Trigger `my_12_sales_by_day` ใส่ช่อง **Partition Key** = `2026-09-20`
   และ Trigger `my_12_customers_by_day` ใส่ `2026-09-21` → dashboard **ไม่รัน** เพราะคนละวัน
3. Trigger `my_12_customers_by_day` อีกครั้งใส่ `2026-09-20` → dashboard รันทันที log: `ส่ง dashboard ของวันที่ 2026-09-20`
   (ป้าย `2026-09-21` ยังรอ sales ของวันนั้นอยู่ ไม่ปนกับวันที่ 20)

💡 ป้ายเป็นแค่ข้อความ ต้องเขียนให้ตรงกันทุก DAG: ใส่ `key_format` เดียวกัน
⚠️ ใส่ Partition Key ผิดรูปแบบ (เช่น `20/09/2026`) จะ Trigger ไม่ได้ เพราะไม่ตรง `key_format`
⚠️ consumer แบบเดิม `schedule=[asset]` **ไม่รัน** จากสัญญาณที่มีป้าย ต้องใช้ `PartitionedAssetTimetable`
⚠️ consumer ที่ pause อยู่ตอนสัญญาณมาถึงจะพลาดสัญญาณนั้นไปเลย (unpause ทีหลังก็ไม่รันย้อน) จึงต้อง unpause consumer ก่อน

## 🧪 ลองเอง

1. สร้าง Asset ตัวที่ 2 `my_daily_customers` ให้ producer มีอีก task ที่ประกาศ แล้วให้ consumer รอ **ทั้ง 2 ตัว**
2. Trigger producer แล้วดูว่า consumer รันเมื่อไหร่
3. (12.5) เพิ่ม Asset `my_products_by_day` และ DAG `my_12_products_by_day` ที่รันตี 5 แล้วให้ dashboard รอ **3 ตัว**
   ลอง Trigger ทั้ง 3 ตัวด้วยป้าย `2026-09-22` แต่ให้ products ใช้ `2026-09-23`: dashboard รันหรือไม่

## ✅ สรุป

| อยาก | เขียน |
|---|---|
| ประกาศว่าข้อมูลอัปเดต | `@task(outlets=[Asset("x")])` |
| รันเมื่อข้อมูลอัปเดต | `@dag(schedule=[Asset("x")])` |
| DAG 1 งาน = Asset 1 ชิ้น | `@asset(schedule=...)` |
| ให้ทุก run มีป้ายวัน | `schedule=CronPartitionTimetable(cron, timezone=..., run_offset=-1, key_format="%Y-%m-%d")` |
| รอหลาย Asset ที่ "วันเดียวกัน" | `schedule=PartitionedAssetTimetable(assets=a & b)` + `context["partition_key"]` |
| สั่ง DAG อื่น | `TriggerDagRunOperator(trigger_dag_id=...)` |
