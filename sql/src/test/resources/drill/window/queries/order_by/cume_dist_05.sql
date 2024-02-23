select c_integer, cume_dist() over(order by c_integer nulls first) from j1 order by cume_dist() over(order by c_integer nulls first), c_integer nulls last;
