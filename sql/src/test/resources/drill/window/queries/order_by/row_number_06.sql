select c_integer, row_number() over(order by c_integer nulls last) from j1 order by c_integer nulls first, row_number() over(order by c_integer nulls last);
