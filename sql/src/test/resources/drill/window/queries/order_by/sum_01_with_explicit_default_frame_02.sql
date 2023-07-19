select c_integer, sum(c_integer) over(order by c_date range unbounded preceding) from j1 order by  2;
