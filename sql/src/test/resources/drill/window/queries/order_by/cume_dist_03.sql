select c_integer, cume_dist() over(order by c_integer desc) from j1 order by 2;
