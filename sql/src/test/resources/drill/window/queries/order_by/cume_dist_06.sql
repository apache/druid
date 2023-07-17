select c_integer, cume_dist() over(order by c_integer nulls last) from j1 order by c_integer nulls first, cume_dist() over(order by c_integer nulls last);
