select c_integer, row_number() over(order by c_integer desc nulls first) from j1 order by 2;
