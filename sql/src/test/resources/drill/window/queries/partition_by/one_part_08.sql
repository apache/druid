select count(c_integer) over(partition by c_double) from j8;
