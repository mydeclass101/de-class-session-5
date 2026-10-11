-- ยอดขายของวันนั้นแยกตามช่องทาง (ไม่นับ order ที่ยกเลิก)
SELECT sales_channel,
       count(*)          AS orders,
       sum(total_amount) AS revenue
FROM orders
WHERE ordered_at >= (%(start)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND ordered_at <  (%(end)s::timestamptz AT TIME ZONE 'Asia/Bangkok')
  AND status <> 'cancelled'
GROUP BY sales_channel
ORDER BY sales_channel;
