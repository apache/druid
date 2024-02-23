select max(c_double) over(partition by c_boolean) from j8;
