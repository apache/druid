-- Same window default definition
-- combination of ranking and row_number function
select
 	rank() 		over W,
	dense_rank()  	over W,
	percent_rank()  over W,
	cume_dist()  	over W,
	row_number()    over W
from
	j6
where
	c_boolean is not null
window	W as (order by c_bigint, c_date, c_time, c_boolean desc nulls first)
order by
	1,
        2,
        3,
	4,
	5
;

