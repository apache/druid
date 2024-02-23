-- Had to comment out avg, running into DRILL-3653
select 
	sum(j1.c_integer) over(partition by j1.c_date order by j1.c_time),
	sum(j1.c_integer) over(),
	sum(j1.c_integer) over(partition by j1.c_bigint, j1.c_date, j1.c_time),
	--avg(j1.c_integer) over(partition by j1.c_date order by j1.c_time),
	--avg(j1.c_integer) over(),
	--avg(j1.c_integer) over(partition by j1.c_bigint, j1.c_date, j1.c_time),
	count(j1.c_integer) over(partition by j1.c_date order by j1.c_time),
	count(j1.c_integer) over(),
	count(j1.c_integer) over(partition by j1.c_bigint, j1.c_date, j1.c_time),
	rank() over(partition by j1.c_date order by j1.c_date ),
	rank() over(partition by j1.c_bigint, j1.c_date, j1.c_time order by j1.c_bigint, j1.c_date, j1.c_time)
from 
	j1, 
	j2 
where 
	j1.c_integer = j2.c_integer 
group by 
	j1.c_date, 
	j1.c_time, 
	j1.c_integer,
	j1.c_bigint;
