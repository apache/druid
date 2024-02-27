-- window function and aggregate function in projection list
select 
        j6.c_integer,
        sum(j6.c_integer) over(partition by j6.c_date order by j6.c_time)
from 
        j6, j7 
where   j6.c_integer = j7.c_integer 
group by 
        j6.c_date, j6.c_time, j6.c_integer
having
	avg(j7.c_integer) > 0;
