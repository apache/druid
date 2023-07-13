select c_integer, dense_rank() over(order by c_integer nulls last) from j1 order by 1, 2;
