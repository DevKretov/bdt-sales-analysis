WITH s1 AS (
SELECT

s.sale_date,
s.item_id,
sum(s.item_cnt_day) as total_count

FROM sales_log s

WHERE to_date(from_unixtime(UNIX_TIMESTAMP(s.sale_date, 'dd.MM.yyyy'))) IN (SELECT MAX(to_date(from_unixtime(UNIX_TIMESTAMP(s11.sale_date, 'dd.MM.yyyy')))) FROM sales_log s11)

GROUP BY s.sale_date, s.item_id
), s2 AS (

SELECT

s.sale_date,
s.item_id,
sum(s.item_cnt_day) as total_count

FROM sales_log s

WHERE date_add(to_date(from_unixtime(UNIX_TIMESTAMP(s.sale_date, 'dd.MM.yyyy'))), 1) IN (SELECT MAX(to_date(from_unixtime(UNIX_TIMESTAMP(s11.sale_date, 'dd.MM.yyyy')))) FROM sales_log s11)

GROUP BY s.sale_date, s.item_id
)

SELECT

s1.sale_date,
s1.item_id,
i.item_name,
ABS(s1.total_count- s2.total_count) as difference

FROM s1 
JOIN s2 ON (s1.item_id = s2.item_id
)
JOIN items i ON (i.item_id = s1.item_id)

ORDER BY difference DESC
LIMIT 10;