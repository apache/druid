select c_integer, cume_dist() over(order by c_integer) from j1 order by 1, 2;
