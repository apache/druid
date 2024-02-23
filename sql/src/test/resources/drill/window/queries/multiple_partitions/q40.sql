-- Common use case is:  SUM( CASE WHEN x=y THEN 1 ELSE 0 END)
select
	b2,
	c2,
	sum(1) 	over(partition by b2 order by c2), 
	sum(1)	over(partition by c2)
from
	t2
order by
	1,2;
