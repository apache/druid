select max(c_double) over(partition by c_bigint order by c_integer, c_date, c_time, c_timestamp nulls first ) from j9;
