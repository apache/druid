-- window function is in order by clause
-- not projected in in select list
select 
	sum(c_integer) over (partition by c_date),
	avg(c_integer) over (partition by c_date),
	avg(c_integer) over (partition by c_date order by c_timestamp),
	avg(c_integer) over ()
from 
	j1 
order by 
	row_number() over(order by c_timestamp);
