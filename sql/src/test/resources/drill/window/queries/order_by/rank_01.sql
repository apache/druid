select c_integer, rank() over(order by c_integer) from j1 order by 1, 2;
