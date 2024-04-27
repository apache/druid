-- Drill 3580
select 
	c_date, 
	c_integer, 
	avg(c_integer + c_bigint) over (partition by c_date), 
	sum(c_bigint - c_integer) over (partition by c_date) 
from 
	j1
;
