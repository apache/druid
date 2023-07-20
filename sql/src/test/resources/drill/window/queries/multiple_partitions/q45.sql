select
	c_integer,
        ntile(5) over(order by c_integer nulls first),
        ntile(10) over(partition by c_time order by c_date)
from
        j2;
