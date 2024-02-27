select c_integer, sum(c_integer) over(partition by c_varchar) from j8 order by c_integer;
