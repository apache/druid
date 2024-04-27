select c_integer, c_date, c_time, c_timestamp, cume_dist() over(partition by c_bigint order by c_integer, c_date, c_time, c_timestamp nulls first ) from j9 order by 1,2,3,4;
