-- จำนวน order ของวันนั้น (เวลาไทย) · SqlSensor ถือว่าผ่านเมื่อค่าแรกไม่ใช่ 0
-- %(start)s / %(end)s มาจาก parameters ใน DAG = data interval ของรอบนี้ (เวลา UTC)
-- AT TIME ZONE 'Asia/Bangkok' แปลงเป็นเวลาไทยแบบไม่มี timezone ให้ตรงกับคอลัมน์ ordered_at
SELECT count(*)
FROM orders
WHERE ordered_at >= (%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND ordered_at <  (%(end)s::timestamptz AT TIME ZONE 'Asia/Bangkok');
