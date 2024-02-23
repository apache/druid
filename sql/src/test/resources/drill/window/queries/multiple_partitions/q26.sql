-- Kitchen sink
-- Use all supported functions
select
 	rank() 				over W,
	dense_rank()  			over W,
	percent_rank()  		over W,
	cume_dist()  			over W,
	avg(c_bigint)  			over W,
	sum(c_integer)  		over W,
	count(c_integer) 		over W,
	count(*) 			over W,
	min(c_integer)  		over W,
	max(c_integer)  		over W,
	row_number()    		over W
from
	j7
where
	c_boolean is not null
window	W as (partition by c_bigint, c_date, c_time, c_boolean order by c_integer)
;

