-- Mix of identical and different window clauses
select
	rank() over (order by c_bigint) 							as rank1,
	dense_rank() over (order by c_date)							as dense_rank1,
	count(*) over (partition by c_date order by c_time) + sum(c_integer) over (partition by c_bigint order by c_time) as total1,
	count(*) over (partition by c_date order by c_time) + avg(c_integer) over() 		as total2,
	sum(c_integer) over (partition by c_date order by c_time) 				as total3,
	avg(c_integer) over (partition by c_date order by c_time) 				as total4,
	rank() over (order by c_bigint) 							as rank2,
	dense_rank() over (order by c_date)							as dense_rank2
from j1;
