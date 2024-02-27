select
	sum(c_bigint - c_integer) over (partition by c_boolean order by c_date),
	sum(c_bigint - c_integer) over (partition by c_date),
	avg(c_bigint - c_integer) over (partition by c_boolean order by c_date),
	avg(c_bigint - c_integer) over (partition by c_date)
from
	j2;
