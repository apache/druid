select c_bigint, avg(c_double) over(partition by c_bigint) from j8 order by c_bigint;
