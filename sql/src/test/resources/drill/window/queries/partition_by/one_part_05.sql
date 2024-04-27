select count(*) over(partition by c_time) from j8;
