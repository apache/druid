select min(c_bigint) over(partition by c_timestamp) from j8;
