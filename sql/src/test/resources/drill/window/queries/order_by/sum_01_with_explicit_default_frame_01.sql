select c_integer, sum(c_integer) over(order by c_date range between unbounded preceding and current row) from j1 order by  2;
