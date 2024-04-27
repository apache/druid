select c_integer, sum(c_integer) over(order by c_date) from j1 order by 1,2;
