select 
	sum(c_integer) over(partition by c_date, c_time, c_timestamp),
	sum(c_integer) over()
from
	j7;
