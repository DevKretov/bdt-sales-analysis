use kretov;
set hive.cli.print.header=true;

SELECT

s.item_id,
i.item_name,
count(s.item_cnt_day) as daily_count

FROM sales_log s
JOIN items i ON (s.item_id = i.item_id)

WHERE to_date(from_unixtime(UNIX_TIMESTAMP(s.sale_date, 'dd.MM.yyyy'))) IN (SELECT MAX(to_date(from_unixtime(UNIX_TIMESTAMP(s1.sale_date, 'dd.MM.yyyy')))) FROM sales_log s1)

GROUP BY s.item_id, i.item_name
ORDER BY daily_count DESC
LIMIT 10;