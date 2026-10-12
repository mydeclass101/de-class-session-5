# Session 5 · Airflow Fundamentals & Core Syntax

เรียนเขียน **Apache Airflow 3.3** (เวอร์ชันล่าสุด) ตั้งแต่ DAG แรกจนถึงการดึงข้อมูลจาก **PostgreSQL**

- data source ของเราเปลี่ยนจาก MySQL มาเป็น **PostgreSQL** แต่ **ข้อมูลร้านค้าชุดเดียวกับ session 3** (ตารางเดียวกัน ตัวเลขเท่ากัน)
- Airflow และ PostgreSQL รันใน **Docker** ส่วนเราเขียนไฟล์ DAG ใน **VS Code** บนเครื่องตัวเอง
- `.venv` ในเครื่องมีไว้ให้ VS Code **เติมโค้ดและแสดงคำอธิบาย syntax** ของ Airflow (Airflow ไม่ได้รันบน Windows)

## 1. สิ่งที่ต้องมี

| โปรแกรม | หมายเหตุ |
|---|---|
| Docker Desktop | ใช้มาตั้งแต่ session 2 · **Settings → Resources → Memory อย่างน้อย 4 GB** |
| Python 3.12 (3.11–3.13 ได้) | ตัวเดียวกับ session 4 |
| VS Code + extension **Python** (Microsoft) | ตัวเดียวกับ session 4 |
| git, DBeaver | |
| พื้นที่ว่าง ~6 GB | image ของ Airflow ~3 GB + ข้อมูล |

## 2. เตรียม repo (ทำก่อนวันเรียน ~20–30 นาที ส่วนใหญ่คือรอดาวน์โหลด)

```bash
git clone git@github.com:mydeclass101/de-class-session-5.git
cd de-class-session-5
code .
```

ใน VS Code เปิด Terminal (`` Ctrl+` ``) แล้วพิมพ์ทีละบรรทัด

**Git Bash**

```powershell
cp .env.example .env
docker compose up -d
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

- `docker compose up -d` ครั้งแรกจะ **ดาวน์โหลด image ของ Airflow (~1 GB)** และ **ไฟล์ข้อมูลร้านค้า (~100 MB) ไปไว้ที่ `data/`** แล้ว restore ลง PostgreSQL
- `pip install` ใช้เวลา 1–2 นาที ติดตั้ง Airflow ลง `.venv` เพื่อให้ VS Code รู้จัก syntax เท่านั้น
- เลือก Python ของ `.venv` ให้ VS Code: `Ctrl+Shift+P` → **Python: Select Interpreter** → `.venv`

ตรวจว่าพร้อม:

```bash
docker compose ps          # ทุกตัวต้องขึ้น (healthy) ยกเว้น airflow-init กับ ecommerce-data ที่จบแล้ว (Exited 0)
```

| หน้าเว็บ / ฐานข้อมูล | ที่อยู่ |
|---|---|
| Airflow | http://localhost:8080 (ไม่ต้อง login: ตั้งไว้สำหรับห้องเรียนเท่านั้น) |
| PostgreSQL `ecommerce` (DBeaver) | `localhost:5432` · database `ecommerce` · user `student` / `student123` |

สุดท้าย ในหน้าเว็บ Airflow หา DAG `lesson_00_setup_check` → กดปุ่ม ▶ (Trigger) → ทุก task ต้องเป็นสีเขียว
หรือรันจาก Terminal:

```bash
docker compose exec airflow-scheduler airflow dags test lesson_00_setup_check
```

ต้องเห็นบรรทัด `ตาราง orders มี 238,424 แถว` = พร้อมเรียน

ครั้งต่อไป: เปิด Docker Desktop → `docker compose up -d` → เปิด VS Code

## 3. ข้อมูลใน PostgreSQL

ตารางเหมือน session 3 ทุกตาราง (`orders`, `order_items`, `customers`, `products`, ...) อยู่ใน schema `public`
ข้อมูลตั้งแต่ 2025-01-01 ถึง **2026-09-24** เวลาเป็นเวลาไทย (ไม่มี timezone) เหมือนเดิม

| schema | สิทธิ์ของ `student` | ใช้ทำอะไร |
|---|---|---|
| `public` | อ่านอย่างเดียว | ข้อมูลร้านค้า (data source ห้ามเขียนกลับ) |
| `lab` | ทำได้ทุกอย่าง | ตารางผลลัพธ์ที่ DAG ของเราสร้าง |

ความต่างจาก MySQL ที่เจอบ่อย: ไม่มี backtick (ใช้ `"ชื่อ"` แทน), `LIMIT` เหมือนกัน, แปลงชนิดด้วย `::` เช่น `'2026-09-20'::date`

## 4. เนื้อหา

เปิดคู่มือในโฟลเดอร์ `lessons/` ทีละบท (ใน VS Code กด `Ctrl+Shift+V` ที่ไฟล์ .md เพื่อดูแบบสวย)
**พิมพ์โค้ดตามคู่มือลงในโฟลเดอร์ `dags/my/`** ไฟล์ตัวอย่างที่เสร็จแล้วอยู่ใน `dags/lessons/` (อย่าแก้)

| บท | คู่มือ | หัวข้อ |
|---|---|---|
| 0 | `00_setup_and_ui.md` | Airflow คืออะไร, ส่วนประกอบ, พาดูหน้าเว็บ, รัน DAG |
| 1 | `01_first_dag.md` | `@dag`, task เปล่า (`EmptyOperator`), ลำดับ `>>` `chain` |
| 2 | `02_tasks_and_operators.md` | `@task`, `@task.bash`, `PythonOperator`, `BashOperator`, `with DAG`, `default_args` |
| 3 | `03_xcom.md` | ส่งค่าระหว่าง task, `multiple_outputs`, `.output`, `xcom_pull` |
| 4 | `04_schedule_and_time.md` | `schedule`, `start_date`, `catchup`, **data interval**, context, template, backfill |
| 5 | `05_branching.md` | `@task.branch`, `@task.short_circuit`, `@task.run_if`, `trigger_rule`, skip |
| 6 | `06_task_groups.md` | `TaskGroup`, `@task_group`, `@setup` / `@teardown` |
| 7 | `07_retries_and_failures.md` | `retries`, timeout, `AirflowFailException`, callback, clear |
| 8 | `08_dynamic_mapping.md` | `.expand()`, `.partial()`, `expand_kwargs` |
| 9 | `09_sensors.md` | `@task.sensor`, `mode="reschedule"`, deferrable |
| 10 | `10_params_variables_connections.md` | `Param`, `Variable`, Connection |
| 11 | `11_postgres.md` | `SQLExecuteQueryOperator`, `PostgresHook`, `SqlSensor`, SQL check |
| 12 | `12_assets_and_triggers.md` | `Asset`, `@asset`, `TriggerDagRunOperator` |

สัญลักษณ์: ⌨️ = พิมพ์ตาม · 🧪 = ลองเอง · 💡 = จำไว้ใช้บ่อย · ⚠️ = ผิดบ่อย · 🔁 = เคยเรียนใน session ก่อน

`CHEATSHEET.md` = syntax Airflow 3 หน้าเดียว + ตาราง **"เจอโค้ดเก่าในเน็ต/ChatGPT → เขียนแบบ Airflow 3 ยังไง"**

## 5. โครงสร้าง repo

```
dags/lessons/       DAG ตัวอย่างของทุกบท (อย่าแก้ เวลาผู้สอนอัปเดตจะ git pull ได้ไม่ชน)
dags/lessons/sql/   ไฟล์ SQL ที่บทที่ 11 ใช้
dags/my/            ที่พิมพ์ DAG ของเราเอง (git ไม่สนใจ)
lessons/            คู่มือแต่ละบท
output/             ไฟล์ที่ DAG เขียนออกมา (ในคอนเทนเนอร์คือ /opt/airflow/output)
data/               ไฟล์ข้อมูลร้านค้า (ดาวน์โหลดเองตอน docker compose up ครั้งแรก)
docker/             สคริปต์ restore ข้อมูล (ไม่ต้องแก้)
```

## 6. คำสั่งที่ใช้บ่อย

```bash
docker compose exec airflow-scheduler airflow dags test <dag_id>        # รัน DAG 1 รอบทันที ดูผลใน Terminal
docker compose exec airflow-scheduler airflow dags list-import-errors   # ไฟล์ DAG ไหนพัง
.venv\Scripts\ruff check --select AIR --preview dags/my                 # หา syntax เก่าของ Airflow 2 (macOS: .venv/bin/ruff)
docker compose logs -f airflow-scheduler                                # ดู log ของ scheduler (Ctrl+C ออก)
```

## 7. เริ่มใหม่ทั้งหมด

```bash
docker compose down -v      # ลบประวัติการรันของ Airflow และฐานข้อมูล ecommerce (โค้ดใน dags/my ไม่หาย)
docker compose up -d        # restore ข้อมูลใหม่ (ไม่ต้องดาวน์โหลดซ้ำ ไฟล์อยู่ใน data/ แล้ว)
```

## 8. แก้ปัญหาที่พบบ่อย

| อาการ | วิธีแก้ |
|---|---|
| `port is already allocated` (8080) | adminer ของ session 3 ใช้ 8080 อยู่: ไปที่โฟลเดอร์ session 3 แล้ว `docker compose stop` หรือแก้ `AIRFLOW_PORT=8081` ใน `.env` |
| `port is already allocated` (5432) | เครื่องมี PostgreSQL อยู่แล้ว: แก้ `POSTGRES_PORT=5433` ใน `.env` แล้ว `docker compose up -d` (DBeaver ใช้ 5433) |
| `ecommerce-data` ขึ้น `Exited (1)` | ดาวน์โหลดไม่สำเร็จ: ดาวน์โหลด `ecommerce_pg.dump` จากหน้า **Releases** ของ repo นี้ วางใน `data/` แล้ว `docker compose up -d` |
| `de5-ecommerce-db` ค้างที่ `health: starting` | ครั้งแรกกำลัง restore ข้อมูล รอได้ ดูด้วย `docker compose logs -f ecommerce-db` (รอ `>>> ecommerce restored`) |
| `de5-ecommerce-db` เป็น `unhealthy` | restore ไม่สำเร็จ: `docker compose down -v` แล้ว `docker compose up -d` |
| สร้างไฟล์ใน `dags/my/` แล้วไม่เห็น DAG ในหน้าเว็บ | รอ ~10–20 วินาที · ถ้ายังไม่เห็นดู `airflow dags list-import-errors` หรือแถบสีแดงบนหน้า Dags |
| `DAG ... not found` ตอน `dags test` | ชื่อ `dag_id` ผิด หรือไฟล์มี error (ดูข้อบน) |
| กด Trigger แล้ว run ค้างที่ `queued` | DAG ถูก pause อยู่: กดสวิตช์ด้านซ้ายของชื่อ DAG ให้เปิด |
| VS Code ขีดเส้นใต้ `airflow` ว่าหาไม่เจอ | ยังไม่ได้เลือก interpreter `.venv` (ข้อ 2) หรือยังไม่ได้ `pip install -r requirements.txt` |
| เครื่องช้ามาก / container restart วน | เพิ่ม Memory ของ Docker Desktop เป็น 4 GB ขึ้นไป และปิด stack ของ session อื่น (`docker compose stop` ในโฟลเดอร์นั้น) |
| `pip` ขึ้น `UnicodeDecodeError` / `cp874` | อย่าเพิ่มภาษาไทยลงใน `requirements.txt` |
