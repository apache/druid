select c_integer, percent_rank() over(order by c_integer nulls first) from j1 order by percent_rank() over(order by c_integer nulls first), c_integer nulls last;
