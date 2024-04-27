select c_integer, avg(c_double) over(partition by c_integer) from j8 order by c_integer;
