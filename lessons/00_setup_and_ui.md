# บทที่ 0 · Airflow คืออะไร และพาดูหน้าเว็บ

## 0.1 ทำไมต้องมี Airflow

🔁 session 4 บทที่ 8 เราเขียน function `run_everything(day)` แล้ว **กดรันเอง** ทีละวัน ของจริงต้องการมากกว่านั้น

| อยากได้ | ถ้าไม่มี Airflow | Airflow ให้ |
|---|---|---|
| รันทุกวันตี 1 เอง | ตั้ง cron บนเครื่องใครสักคน | `schedule` |
| ขั้น B รอขั้น A เสร็จก่อน | เขียน if เอง | ลำดับ `a >> b` |
| เน็ตหลุด ลองใหม่ 3 ครั้ง | เขียน loop ลองใหม่เอง (session 4 บท 5.4) | `retries=3` |
| รันย้อนหลัง 30 วัน | loop วันที่เอง | `catchup` / backfill |
| รู้ว่าเมื่อคืนอะไรพัง ดู log ที่ไหน | ไล่หาไฟล์ log | หน้าเว็บ Grid + Logs |

💡 **Airflow ไม่ได้ประมวลผลข้อมูลเอง** เป็นตัว "สั่ง" ให้โค้ดของเรารันตามเวลาและลำดับ (orchestrator)

## 0.2 คำศัพท์ 4 คำ

| คำ | ความหมาย |
|---|---|
| **DAG** | งาน 1 ชุด = กล่องหลายกล่องต่อกันด้วยลูกศรทางเดียว ไม่วนกลับ (Directed Acyclic Graph) 1 ไฟล์ .py มีได้หลาย DAG |
| **Task** | กล่อง 1 กล่องใน DAG = งาน 1 ขั้น |
| **Operator** | แม่แบบของ task สำเร็จรูป เช่น `BashOperator` (รันคำสั่ง bash), `SQLExecuteQueryOperator` (รัน SQL) |
| **DAG run** | การรัน DAG 1 รอบ เช่น รอบของวันที่ 20 ก.ย. แต่ละ task ในรอบนั้นเรียกว่า **task instance** |

## 0.3 ส่วนประกอบของ Airflow 3 (ดูได้จาก `docker compose ps`)

```
  dags/*.py ──► dag-processor ──► airflow-db (metadata) ◄── scheduler ──► รัน task (LocalExecutor)
  (ไฟล์ของเรา)   อ่านไฟล์ → DAG       จดทุกอย่าง               ดูเวลา/ลำดับ        │
                                         ▲                                         ▼
                 เรา ──► api-server (หน้าเว็บ :8080) ─┘                triggerer: เฝ้า task ที่ "พักรอ"
```

| service | หน้าที่ |
|---|---|
| `airflow-dag-processor` | อ่านไฟล์ใน `dags/` ทุก ~10 วินาที แปลงเป็น DAG (ไฟล์พัง = import error) |
| `airflow-scheduler` | ถึงเวลาแล้วหรือยัง task ไหนพร้อมรัน แล้วสั่งรัน |
| `airflow-apiserver` | หน้าเว็บ + REST API |
| `airflow-triggerer` | เฝ้ารอแทน task ที่พักรอ (deferrable) บทที่ 9 |
| `airflow-db` | PostgreSQL ที่ Airflow จดสถานะทุกอย่าง (ไม่ใช่ข้อมูลร้านค้า) |
| `ecommerce-db` | PostgreSQL ข้อมูลร้านค้า = data source ของเรา |

⚠️ โค้ด **ระดับบนสุด** ของไฟล์ DAG (นอก function) ถูกรันทุกครั้งที่ dag-processor อ่านไฟล์ (ทุก ~10 วินาที)
ห้ามต่อฐานข้อมูลหรือเรียก API ตรงนั้น ให้ทำใน task เท่านั้น

## 0.4 พาดูหน้าเว็บ http://localhost:8080

1. **Dags**: รายชื่อ DAG · สวิตช์ซ้ายมือ = pause/unpause (DAG ใหม่ถูก pause ไว้ก่อน) · ▶ = Trigger (รันเดี๋ยวนี้)
2. คลิกชื่อ `lesson_00_setup_check` → แท็บ **Graph** = หน้าตา DAG · แท็บ **Code** = โค้ดของไฟล์
   (ตอนนี้มี DAG นี้ตัวเดียว DAG ที่เราพิมพ์ใน `dags/my/` จะมาต่อท้ายรายการนี้)
3. กด ▶ **Trigger** → แท็บ **Grid**: 1 คอลัมน์ = 1 run, 1 ช่อง = 1 task instance
4. คลิกช่องสีเขียว → **Logs** (ดู print / error) · **XCom** (ค่าที่ task ส่งต่อ, บทที่ 3)
5. เมนู **Admin** → Variables / Connections (บทที่ 10)

สถานะที่เจอบ่อย (เอาเมาส์ชี้ช่องจะเห็นชื่อสถานะ):

| สถานะ | ความหมาย |
|---|---|
| `success` | สำเร็จ |
| `failed` | พัง |
| `running` | กำลังรัน |
| `queued` / `scheduled` | รอคิว |
| `up_for_retry` | พังแล้ว รอลองใหม่ (บทที่ 7) |
| `upstream_failed` | ตัวก่อนหน้าพัง เลยไม่ได้รัน |
| `skipped` | ถูกข้าม (บทที่ 5) |
| `up_for_reschedule` / `deferred` | sensor กำลังรอ (บทที่ 9) |

## 0.5 รันจาก Terminal

```bash
docker compose exec airflow-scheduler airflow dags test lesson_00_setup_check
```

- `dags test` = รัน DAG **1 รอบทันที** ใน Terminal ไม่สนว่า pause อยู่ เห็น print ทันที ใช้ตอนเขียนโค้ด
- ผลลัพธ์ยาวมาก หาบรรทัดที่ไม่มีวันที่นำหน้า (print ของเรา) และบรรทัดท้าย `DagRun Finished: ... state=success`
- `docker compose exec airflow-scheduler` = "เข้าไปสั่งในคอนเทนเนอร์ scheduler" (session 2)

## 0.6 เปิดไฟล์ DAG แรก

เปิด `dags/lesson_00_setup_check.py` ใน VS Code แล้วลองเอาเมาส์ชี้ที่ `dag`, `task`, `PostgresHook`
ต้องเห็นคำอธิบายขึ้นมา (มาจาก `.venv`) ถ้าไม่เห็นดู README ข้อ 8

💡 ตัวอย่างเต็มของทุกบทอยู่ใน `examples/` (อยู่นอก `dags/` จึงไม่ขึ้นหน้าเว็บ) เปิดอ่านใน VS Code ได้เลย
อยากรันดูให้คัดลอกลง `dags/my/` เช่น `cp examples/lesson_01_first_dag.py dags/my/` (README ข้อ 4)

🧪 ในหน้าเว็บ เปิด log ของ task `count_orders` หาบรรทัด `ตาราง orders มี ... แถว` แล้วไปดูแท็บ XCom ของ task เดียวกัน
