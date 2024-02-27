select c_integer, percent_rank() over(order by c_integer desc) from j1 order by 2;
