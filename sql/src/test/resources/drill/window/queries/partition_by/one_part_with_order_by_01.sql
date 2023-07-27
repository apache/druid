select c_integer, sum(c_integer) over(partition by c_varchar order by c_integer) from j8 order by c_integer;
