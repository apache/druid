select c_integer, percent_rank() over(order by c_integer nulls last) from j1 order by c_integer nulls first, percent_rank() over(order by c_integer nulls last);
