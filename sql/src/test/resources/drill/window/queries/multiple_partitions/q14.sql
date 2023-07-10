-- Multipe window definitions
-- Different aggregate window functions
-- One of the window definitions is empty
select 
	c_integer,
	c_date,
	c_time,
	sum(c_integer) over(),
	avg(c_integer) over(partition by c_time order by c_date desc nulls first)
from
	j7;
