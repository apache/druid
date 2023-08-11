select c_integer, percent_rank() over(order by c_integer) from j1 order by 1, 2;
