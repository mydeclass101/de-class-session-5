-- เก็บยอดขายรายวันลงตาราง lab.daily_sales (schema lab = ที่ของเรา เขียนได้)
-- ลบของวันนั้นก่อนแล้วใส่ใหม่: รันวันเดิมซ้ำกี่ครั้งก็ไม่เกิดแถวซ้ำ (เหตุผลละเอียดใน session 6)
CREATE TABLE IF NOT EXISTS lab.daily_sales (
    sales_date    date          NOT NULL,
    sales_channel varchar(10)   NOT NULL,
    orders        integer       NOT NULL,
    revenue       numeric(14,2) NOT NULL,
    loaded_at     timestamp     NOT NULL DEFAULT now(),
    PRIMARY KEY (sales_date, sales_channel)
);

DELETE FROM lab.daily_sales
WHERE sales_date = (%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok')::date;

INSERT INTO lab.daily_sales (sales_date, sales_channel, orders, revenue)
SELECT (%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok')::date,
       sales_channel, count(*), sum(total_amount)
FROM orders
WHERE ordered_at >= (%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND ordered_at <  (%(end)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND status <> 'cancelled'
GROUP BY sales_channel;
