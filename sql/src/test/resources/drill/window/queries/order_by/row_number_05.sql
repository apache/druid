select c_integer, row_number() over(order by c_integer nulls first) from j1 order by row_number() over(order by c_integer nulls first), c_integer nulls last;
