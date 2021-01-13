SELECT 

c.item_category_id,
c.item_category_name,
sum(s.item_cnt_day) as num_sold,
sum(s.item_cnt_day * s.item_price) as sales

FROM sales_log s 
JOIN items i ON (s.item_id = i.item_id) 
JOIN item_categories c ON (c.item_category_id = i.item_category_id) 

WHERE to_date(from_unixtime(UNIX_TIMESTAMP(s.sale_date, 'dd.MM.yyyy'))) IN (SELECT MAX(to_date(from_unixtime(UNIX_TIMESTAMP(s1.sale_date, 'dd.MM.yyyy')))) FROM sales_log s1)
GROUP BY c.item_category_id, c.item_category_name
ORDER BY sales;