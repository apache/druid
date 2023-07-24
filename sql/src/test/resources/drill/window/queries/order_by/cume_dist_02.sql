select c_integer as a, cume_dist() over(order by c_integer) as b from j1 order by  a, b desc;
