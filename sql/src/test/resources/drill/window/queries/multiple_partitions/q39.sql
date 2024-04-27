select
	sum(c_integer + 100)  over (partition by c_boolean order by c_date),
	sum(c_integer - 100)  over (partition by c_date),
	avg(c_integer - 1000) over (partition by c_boolean order by c_date),
	avg(c_integer - 1000) over (partition by c_date),
	avg(c_integer) 	      	over(),
	row_number() 		over(order by c_date)
from
	j2;
