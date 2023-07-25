select
	last_value(c_integer) over(order by c_integer nulls first),
	last_value(c_bigint) over(partition by c_time),
	last_value(c_integer) over (partition by c_time),
	last_value(c_bigint) over(partition by c_time order by c_date),
	last_value(c_integer) over (partition by c_time order by c_date)
from
	j7;
