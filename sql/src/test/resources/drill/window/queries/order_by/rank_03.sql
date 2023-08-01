select c_integer, rank() over(order by c_integer desc) from j1 order by 1, 2;
