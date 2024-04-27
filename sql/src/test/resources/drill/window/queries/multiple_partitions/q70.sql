-- Drill 3580
select 
	c_date, 
	c_integer, 
	count(c_integer) over (partition by c_date), 
	count(c_bigint - c_integer) over (partition by c_date),
	count(*) over (partition by c_date) 
from 
	j1
;
