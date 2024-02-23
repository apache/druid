-- window function in case statement
-- multiple different windows
select 
	*,
	case when avg(c_integer) over(partition by c_varchar) < c_bigint then 1000 end,
	case when avg(c_bigint)  over(partition by c_date, c_time order by c_varchar) < 0 then   -1000 end
from 
	j6;
