select c_bigint, min(c_double) over(partition by c_bigint order by c_date, c_time nulls first) from j9;
