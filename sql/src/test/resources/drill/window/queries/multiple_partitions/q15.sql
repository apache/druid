-- Different window definitions
-- Aggregate and row_number window functions
select 
	c_integer,
	c_date,
	c_time,
	row_number() over(order by c_date),
	avg(c_integer) over(partition by c_time order by c_date desc nulls first)
from
	j1
order by
	row_number() over(order by c_date);
