select 
	c_date,
	c_time,
	sum(c_integer) over(partition by c_date, c_time),
	sum(c_integer) over(partition by c_time order by c_date desc)
from
	j7;
