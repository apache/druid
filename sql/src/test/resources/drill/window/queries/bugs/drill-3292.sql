select 
	sum(1) 	over w sum1, 
	sum(5) 	over w sum5,
	sum(10) over w sum10
from 
	j1
where 
	c_date is not null
window w as (partition by c_date);
