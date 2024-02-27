-- Multiple window definition where parameter to aggregate window function is COALESCE function 
select 
	sum(coalesce(c_bigint, 100)) over(), 
	avg(coalesce(c_bigint, 100)) over(), 
	count(coalesce(c_bigint, 100)) over(), 
	min(coalesce(c_bigint, 100)) over(), 
	max(coalesce(c_bigint, 100)) over(), 
	sum(coalesce(c_integer, 100)) over(partition by c_date),
	avg(coalesce(c_integer, 100)) over(partition by c_date),
	count(coalesce(c_integer, 100)) over(partition by c_date), 
	min(coalesce(c_integer, 100)) over(partition by c_date), 
	max(coalesce(c_integer, 100)) over(partition by c_date)
from 
	j1;					
