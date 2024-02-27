select 
	c_date,
	c_time,
	rank() over(partition by c_date order by c_time nulls first),
	rank() over(partition by c_time order by c_time nulls first)
from
	j6
order by
	1,2 nulls first;
