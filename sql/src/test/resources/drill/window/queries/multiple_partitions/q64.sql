-- Drill 3580
select 
	c_date, 
	c_integer, 
	sum(c_integer) over (partition by c_date), 
	sum(c_integer + c_bigint) over (partition by c_date) 
from 
	j1
;
