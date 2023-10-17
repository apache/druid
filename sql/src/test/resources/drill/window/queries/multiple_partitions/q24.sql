-- Same window default definition
-- combination of aggregate and row_number function
select
 	count(*) 	over W ,
	avg(c_integer + 100)  over W  + sum(c_integer + 1.0) over W,
	row_number()    over W 
from
	j6
where
	c_boolean is not null
window	W as (partition by c_bigint, c_date, c_time order by c_integer, c_boolean)
order by
	1, 2, 3;

