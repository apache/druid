select count(*) over(partition by c_bigint, c_date, c_boolean) from j8;
