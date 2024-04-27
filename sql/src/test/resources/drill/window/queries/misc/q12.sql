-- window clause
select  
	sum(c_integer) over w,
        avg(c_bigint) over w,
        count(*) over w
from    j7
window w as (partition by c_date order by c_time desc)
order by 1, 2, 3;
