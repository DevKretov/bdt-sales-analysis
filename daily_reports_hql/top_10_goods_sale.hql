WITH s1 AS (
SELECT

s.sale_date,
s.item_id,
s.item_cnt_day * s.item_price AS shop_sale

FROM sales_log s
WHERE to_date(from_unixtime(UNIX_TIMESTAMP(s.sale_date, 'dd.MM.yyyy'))) IN (SELECT MAX(to_date(from_unixtime(UNIX_TIMESTAMP(s1.sale_date, 'dd.MM.yyyy')))) FROM sales_log s1)
)

SELECT

s1.item_id,
i.item_name,
sum(s1.shop_sale) as sale

FROM s1
JOIN items i ON (i.item_id = s1.item_id)

GROUP BY s1.item_id, i.item_name
ORDER BY sale DESC
LIMIT 10;