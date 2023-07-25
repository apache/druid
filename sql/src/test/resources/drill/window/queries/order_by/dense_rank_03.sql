select c_integer, dense_rank() over(order by c_integer desc) from j1 order by 1, 2;
