select count(*) over(partition by c_bigint order by c_timestamp) from j8;
