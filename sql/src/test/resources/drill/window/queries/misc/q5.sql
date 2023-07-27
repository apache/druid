select sum(j1.c_integer) over(partition by j1.c_date order by j1.c_time)  from j1, j2 where j1.c_integer = j2.c_integer group by j1.c_date, j1.c_time, j1.c_integer;
