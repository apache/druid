select c_integer, row_number() over(order by c_integer) from j1 order by 2 desc;
